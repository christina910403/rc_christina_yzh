package storage

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/christina910403/rc_christina_yzh/internal/domain"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrIdempotencyConflict = errors.New("idempotency conflict")
	ErrInvalidState        = errors.New("invalid state")
)

//go:embed migrations/*.sql
var migrations embed.FS

type Postgres struct{ pool *pgxpool.Pool }

func Open(ctx context.Context, databaseURL string) (*Postgres, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Postgres{pool: pool}, nil
}

func (p *Postgres) Close()                         { p.pool.Close() }
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

func (p *Postgres) Migrate(ctx context.Context) error {
	entries, err := migrations.ReadDir("migrations")
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		sqlBytes, err := migrations.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return err
		}
		if _, err := p.pool.Exec(ctx, string(sqlBytes)); err != nil {
			return fmt.Errorf("apply migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

func (p *Postgres) AcceptEvent(ctx context.Context, record domain.EventRecord, fingerprint string, drafts []domain.DeliveryDraft) (domain.EventRecord, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return domain.EventRecord{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	payload, err := json.Marshal(record.Payload)
	if err != nil {
		return domain.EventRecord{}, fmt.Errorf("marshal payload: %w", err)
	}
	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO event_instances
		(id, source_system, event_id, event_type, occurred_at, routing_key, payload, request_fingerprint, config_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (source_system, event_id) DO NOTHING
		RETURNING created_at`, record.ID, record.SourceSystem, record.EventID, record.EventType,
		record.OccurredAt, record.RoutingKey, payload, fingerprint, record.ConfigVersion).Scan(&createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		var existingID, existingFingerprint string
		if err := tx.QueryRow(ctx, `SELECT id, request_fingerprint FROM event_instances WHERE source_system=$1 AND event_id=$2`, record.SourceSystem, record.EventID).Scan(&existingID, &existingFingerprint); err != nil {
			return domain.EventRecord{}, err
		}
		if existingFingerprint != fingerprint {
			return domain.EventRecord{}, ErrIdempotencyConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return domain.EventRecord{}, err
		}
		existing, err := p.GetEvent(ctx, existingID)
		if err == nil {
			existing.Duplicate = true
		}
		return existing, err
	}
	if err != nil {
		return domain.EventRecord{}, err
	}

	for _, draft := range drafts {
		snapshot, err := json.Marshal(draft.Snapshot)
		if err != nil {
			return domain.EventRecord{}, fmt.Errorf("marshal request snapshot: %w", err)
		}
		var next any
		if draft.Status == domain.StatusAccepted {
			next = time.Now().UTC()
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO deliveries
			(id,event_pk,route_id,route_revision,external_system_id,api_operation_id,api_operation_revision,
			 request_snapshot,status,max_attempts,next_attempt_at,last_error_class,last_error_summary)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
			draft.ID, record.ID, draft.RouteID, draft.RouteRevision, draft.ExternalSystemID,
			draft.APIOperationID, draft.APIOperationRevision, snapshot, draft.Status,
			draft.MaxAttempts, next, nullIfEmpty(draft.LastErrorClass), nullIfEmpty(draft.LastErrorSummary))
		if err != nil {
			return domain.EventRecord{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.EventRecord{}, err
	}
	record.CreatedAt = createdAt
	record.Deliveries, err = p.listDeliveriesForEvent(ctx, record.ID)
	return record, err
}

func (p *Postgres) GetEvent(ctx context.Context, id string) (domain.EventRecord, error) {
	var r domain.EventRecord
	var payload []byte
	err := p.pool.QueryRow(ctx, `
		SELECT id,source_system,event_id,event_type,occurred_at,routing_key,payload,request_fingerprint,config_version,created_at
		FROM event_instances WHERE id=$1`, id).Scan(&r.ID, &r.SourceSystem, &r.EventID, &r.EventType, &r.OccurredAt, &r.RoutingKey, &payload, &r.Fingerprint, &r.ConfigVersion, &r.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.EventRecord{}, ErrNotFound
	}
	if err != nil {
		return domain.EventRecord{}, err
	}
	if err := json.Unmarshal(payload, &r.Payload); err != nil {
		return domain.EventRecord{}, err
	}
	r.Deliveries, err = p.listDeliveriesForEvent(ctx, id)
	return r, err
}

func (p *Postgres) GetDelivery(ctx context.Context, id string) (domain.Delivery, error) {
	row := p.pool.QueryRow(ctx, deliverySelect+` WHERE id=$1`, id)
	d, err := scanDelivery(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Delivery{}, ErrNotFound
	}
	return d, err
}

func (p *Postgres) ListAttempts(ctx context.Context, deliveryID string) ([]domain.DeliveryAttempt, error) {
	rows, err := p.pool.Query(ctx, `SELECT round,attempt_no,started_at,finished_at,duration_ms,http_status,result_class,COALESCE(error_summary,'') FROM delivery_attempts WHERE delivery_id=$1 ORDER BY round,attempt_no`, deliveryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.DeliveryAttempt
	for rows.Next() {
		var a domain.DeliveryAttempt
		if err := rows.Scan(&a.Round, &a.AttemptNo, &a.StartedAt, &a.FinishedAt, &a.DurationMS, &a.HTTPStatus, &a.ResultClass, &a.ErrorSummary); err != nil {
			return nil, err
		}
		result = append(result, a)
	}
	return result, rows.Err()
}

func (p *Postgres) ListDeadLetters(ctx context.Context, limit int) ([]domain.Delivery, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	rows, err := p.pool.Query(ctx, deliverySelect+` WHERE status='DEAD_LETTER' ORDER BY updated_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func (p *Postgres) ClaimDeliveries(ctx context.Context, owner string, batch int, lease time.Duration) ([]domain.Delivery, error) {
	if _, err := p.pool.Exec(ctx, `
		UPDATE deliveries SET status='DEAD_LETTER',last_error_class='LEASE_EXHAUSTED',
		last_error_summary='worker lease expired on final attempt',lease_owner=NULL,lease_until=NULL,updated_at=clock_timestamp(),completed_at=clock_timestamp()
		WHERE status='DELIVERING' AND lease_until <= clock_timestamp() AND attempt_count >= max_attempts`); err != nil {
		return nil, err
	}

	rows, err := p.pool.Query(ctx, `
		WITH picked AS (
			SELECT id FROM deliveries
			WHERE ((status IN ('ACCEPTED','RETRY_WAIT') AND next_attempt_at <= clock_timestamp())
			   OR (status='DELIVERING' AND lease_until <= clock_timestamp()))
			  AND attempt_count < max_attempts
			ORDER BY next_attempt_at NULLS FIRST,id
			FOR UPDATE SKIP LOCKED LIMIT $1
		)
		UPDATE deliveries d SET status='DELIVERING',lease_owner=$2,
			lease_until=clock_timestamp()+($3::bigint * interval '1 millisecond'),
			attempt_count=d.attempt_count+1,updated_at=clock_timestamp()
		FROM picked WHERE d.id=picked.id
		RETURNING d.id,d.event_pk,d.route_id,d.route_revision,d.external_system_id,d.api_operation_id,d.api_operation_revision,
			d.request_snapshot,d.status,d.round,d.attempt_count,d.max_attempts,d.next_attempt_at,d.lease_owner,
			COALESCE(d.last_error_class,''),COALESCE(d.last_error_summary,''),d.created_at,d.updated_at`, batch, owner, lease.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func (p *Postgres) FinishAttempt(ctx context.Context, result domain.AttemptResult) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	_, err = tx.Exec(ctx, `
		INSERT INTO delivery_attempts(delivery_id,round,attempt_no,started_at,finished_at,duration_ms,http_status,result_class,error_summary)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT(delivery_id,round,attempt_no) DO NOTHING`, result.DeliveryID, result.Round, result.AttemptNo,
		result.StartedAt, result.FinishedAt, result.FinishedAt.Sub(result.StartedAt).Milliseconds(), result.HTTPStatus, result.ResultClass, nullIfEmpty(result.ErrorSummary))
	if err != nil {
		return err
	}
	command, err := tx.Exec(ctx, `
		UPDATE deliveries SET status=$1,next_attempt_at=$2,lease_owner=NULL,lease_until=NULL,
		last_error_class=$3,last_error_summary=$4,updated_at=clock_timestamp(),
		completed_at=CASE WHEN $1 IN ('SUCCEEDED','DEAD_LETTER') THEN clock_timestamp() ELSE NULL END
		WHERE id=$5 AND status='DELIVERING' AND lease_owner=$6 AND round=$7 AND attempt_count=$8`,
		result.FinalStatus, result.NextAttempt, nullIfEmpty(result.ResultClass), nullIfEmpty(result.ErrorSummary),
		result.DeliveryID, result.LeaseOwner, result.Round, result.AttemptNo)
	if err != nil {
		return err
	}
	if command.RowsAffected() != 1 {
		return fmt.Errorf("finish attempt lost lease: %w", ErrInvalidState)
	}
	return tx.Commit(ctx)
}

func (p *Postgres) Replay(ctx context.Context, deliveryID, operator, reason string, snapshot *domain.RequestSnapshot) (domain.Delivery, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return domain.Delivery{}, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var snapshotJSON any
	var routeRevision, operationRevision any
	var externalSystemID, operationID any
	if snapshot != nil {
		encoded, err := json.Marshal(snapshot)
		if err != nil {
			return domain.Delivery{}, err
		}
		snapshotJSON = encoded
		routeRevision = snapshot.RouteRevision
		operationRevision = snapshot.APIOperationRevision
		externalSystemID = snapshot.ExternalSystemID
		operationID = snapshot.APIOperationID
	}
	var fromRound, toRound int
	err = tx.QueryRow(ctx, `
		UPDATE deliveries SET status='ACCEPTED',round=round+1,attempt_count=0,next_attempt_at=clock_timestamp(),
		lease_owner=NULL,lease_until=NULL,last_error_class=NULL,last_error_summary=NULL,completed_at=NULL,updated_at=clock_timestamp(),
		request_snapshot=COALESCE($2::jsonb,request_snapshot),route_revision=COALESCE($3::integer,route_revision),
		api_operation_revision=COALESCE($4::integer,api_operation_revision),
		external_system_id=COALESCE($5::varchar,external_system_id),api_operation_id=COALESCE($6::varchar,api_operation_id)
		WHERE id=$1 AND status='DEAD_LETTER' RETURNING round-1,round`, deliveryID, snapshotJSON, routeRevision, operationRevision, externalSystemID, operationID).Scan(&fromRound, &toRound)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Delivery{}, ErrInvalidState
	}
	if err != nil {
		return domain.Delivery{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO delivery_replays(delivery_id,from_round,to_round,operator,reason) VALUES($1,$2,$3,$4,$5)`, deliveryID, fromRound, toRound, operator, reason)
	if err != nil {
		return domain.Delivery{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.Delivery{}, err
	}
	return p.GetDelivery(ctx, deliveryID)
}

const deliverySelect = `SELECT id,event_pk,route_id,route_revision,external_system_id,api_operation_id,api_operation_revision,
	request_snapshot,status,round,attempt_count,max_attempts,next_attempt_at,COALESCE(lease_owner,''),
	COALESCE(last_error_class,''),COALESCE(last_error_summary,''),created_at,updated_at FROM deliveries`

type rowScanner interface{ Scan(dest ...any) error }

func scanDelivery(row rowScanner) (domain.Delivery, error) {
	var d domain.Delivery
	var snapshot []byte
	err := row.Scan(&d.ID, &d.EventPK, &d.RouteID, &d.RouteRevision, &d.ExternalSystemID, &d.APIOperationID, &d.APIOperationRevision,
		&snapshot, &d.Status, &d.Round, &d.AttemptCount, &d.MaxAttempts, &d.NextAttemptAt, &d.LeaseOwner,
		&d.LastErrorClass, &d.LastErrorSummary, &d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return domain.Delivery{}, err
	}
	if err := json.Unmarshal(snapshot, &d.Snapshot); err != nil {
		return domain.Delivery{}, err
	}
	return d, nil
}

func (p *Postgres) listDeliveriesForEvent(ctx context.Context, eventID string) ([]domain.Delivery, error) {
	rows, err := p.pool.Query(ctx, deliverySelect+` WHERE event_pk=$1 ORDER BY route_id`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Delivery
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

func nullIfEmpty(value string) any {
	if value == "" {
		return nil
	}
	return value
}
