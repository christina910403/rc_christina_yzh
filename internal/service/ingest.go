package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/christina910403/rc_christina_yzh/internal/config"
	"github.com/christina910403/rc_christina_yzh/internal/domain"
	"github.com/christina910403/rc_christina_yzh/internal/idgen"
	"github.com/christina910403/rc_christina_yzh/internal/requesttpl"
)

var (
	ErrUnknownEvent   = errors.New("unknown event type")
	ErrForbiddenEvent = errors.New("source cannot publish event type")
	ErrInvalidPayload = errors.New("invalid event payload")
	ErrNoRoute        = errors.New("event requires at least one route")
	ErrRouteNotFound  = errors.New("route is not available in current configuration")
	ErrReplayStore    = errors.New("store does not support replay")
)

type EventStore interface {
	AcceptEvent(context.Context, domain.EventRecord, string, []domain.DeliveryDraft) (domain.EventRecord, error)
}

type ReplayStore interface {
	GetDelivery(context.Context, string) (domain.Delivery, error)
	GetEvent(context.Context, string) (domain.EventRecord, error)
	Replay(context.Context, string, string, string, *domain.RequestSnapshot) (domain.Delivery, error)
}

type Ingestor struct {
	config *config.Runtime
	store  EventStore
}

func NewIngestor(runtime *config.Runtime, store EventStore) *Ingestor {
	return &Ingestor{config: runtime, store: store}
}

func (i *Ingestor) Accept(ctx context.Context, source string, event domain.EventInput) (domain.EventRecord, error) {
	definition, ok := i.config.Events[event.EventType]
	if !ok {
		return domain.EventRecord{}, ErrUnknownEvent
	}
	if definition.Definition.Source != source {
		return domain.EventRecord{}, ErrForbiddenEvent
	}
	if _, err := i.config.ValidateEvent(source, event); err != nil {
		return domain.EventRecord{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
	}
	routes := i.config.MatchRoutes(event)
	if len(routes) == 0 && definition.Definition.RequireRoute {
		return domain.EventRecord{}, ErrNoRoute
	}

	eventPK, err := idgen.New()
	if err != nil {
		return domain.EventRecord{}, err
	}
	fingerprint, err := fingerprintEvent(event)
	if err != nil {
		return domain.EventRecord{}, err
	}
	record := domain.EventRecord{
		ID: eventPK, SourceSystem: source, EventID: event.EventID, EventType: event.EventType,
		OccurredAt: event.OccurredAt, RoutingKey: event.RoutingKey, Payload: event.Payload,
		ConfigVersion: i.config.Version,
	}
	drafts := make([]domain.DeliveryDraft, 0, len(routes))
	for _, route := range routes {
		draft, err := i.renderDelivery(event, route)
		if err != nil {
			deliveryID, idErr := idgen.New()
			if idErr != nil {
				return domain.EventRecord{}, idErr
			}
			draft = domain.DeliveryDraft{
				ID: deliveryID, RouteID: route.Definition.ID, RouteRevision: route.Definition.Revision,
				ExternalSystemID: route.Operation.ExternalSystem.ID,
				APIOperationID:   route.Operation.Definition.ID, APIOperationRevision: route.Operation.Definition.Revision,
				Status: domain.StatusDeadLetter, MaxAttempts: maxAttempts(i.config, route.Operation),
				LastErrorClass: "CONFIG_ERROR", LastErrorSummary: truncate(err.Error(), 512),
			}
		}
		drafts = append(drafts, draft)
	}
	return i.store.AcceptEvent(ctx, record, fingerprint, drafts)
}

func (i *Ingestor) Replay(ctx context.Context, deliveryID, operator, reason string, useCurrentConfig bool) (domain.Delivery, error) {
	store, ok := i.store.(ReplayStore)
	if !ok {
		return domain.Delivery{}, ErrReplayStore
	}
	if !useCurrentConfig {
		return store.Replay(ctx, deliveryID, operator, reason, nil)
	}
	delivery, err := store.GetDelivery(ctx, deliveryID)
	if err != nil {
		return domain.Delivery{}, err
	}
	event, err := store.GetEvent(ctx, delivery.EventPK)
	if err != nil {
		return domain.Delivery{}, err
	}
	var selected *config.RuntimeRoute
	for _, route := range i.config.RoutesByEvent[event.EventType] {
		if route.Definition.ID == delivery.RouteID && route.Definition.Enabled {
			selected = route
			break
		}
	}
	if selected == nil {
		return domain.Delivery{}, ErrRouteNotFound
	}
	draft, err := i.renderDelivery(domain.EventInput{
		EventID: event.EventID, EventType: event.EventType, OccurredAt: event.OccurredAt,
		RoutingKey: event.RoutingKey, Payload: event.Payload,
	}, selected)
	if err != nil {
		return domain.Delivery{}, fmt.Errorf("render current route: %w", err)
	}
	return store.Replay(ctx, deliveryID, operator, reason, &draft.Snapshot)
}

func (i *Ingestor) renderDelivery(event domain.EventInput, route *config.RuntimeRoute) (domain.DeliveryDraft, error) {
	rendered, err := route.Template.Render(requesttpl.EventContext{
		EventID: event.EventID, EventType: event.EventType, OccurredAt: event.OccurredAt,
		RoutingKey: event.RoutingKey, Payload: event.Payload,
	})
	if err != nil {
		return domain.DeliveryDraft{}, err
	}
	op := route.Operation
	u, err := url.Parse(op.URL)
	if err != nil {
		return domain.DeliveryDraft{}, err
	}
	query := u.Query()
	for name, values := range rendered.Query {
		for _, value := range values {
			query.Add(name, value)
		}
	}
	u.RawQuery = query.Encode()

	headers := map[string]string{}
	for name, value := range op.Definition.StaticHeaders {
		headers[http.CanonicalHeaderKey(name)] = value
	}
	for name, value := range rendered.Headers {
		canonical := http.CanonicalHeaderKey(name)
		if _, exists := headers[canonical]; exists {
			return domain.DeliveryDraft{}, fmt.Errorf("dynamic header %s conflicts with static header", canonical)
		}
		headers[canonical] = value
	}
	if _, exists := headers["Content-Type"]; !exists {
		headers["Content-Type"] = rendered.BodyEncoding
	}
	secretHeaders := map[string]string{}
	for name, ref := range op.Definition.SecretHeaders {
		secretHeaders[http.CanonicalHeaderKey(name)] = ref
	}

	deliveryID, err := idgen.New()
	if err != nil {
		return domain.DeliveryDraft{}, err
	}
	timeout := op.Definition.TimeoutMS
	if timeout <= 0 {
		timeout = 10000
	}
	snapshot := domain.RequestSnapshot{
		URL: u.String(), Method: op.Definition.Method, Headers: headers, SecretHeaders: secretHeaders,
		Body: rendered.Body, BodyEncoding: rendered.BodyEncoding, TimeoutMS: timeout,
		RetryDelaysMS: op.RetryDelaysMS, IdempotencyHeader: op.Definition.IdempotencyHeader,
		EventID: event.EventID, ExternalSystemID: op.ExternalSystem.ID,
		APIOperationID: op.Definition.ID, APIOperationRevision: op.Definition.Revision,
		RouteID: route.Definition.ID, RouteRevision: route.Definition.Revision,
		MaxConcurrency: op.ExternalSystem.MaxConcurrency, RateLimitPerSecond: op.ExternalSystem.RateLimitPerSecond,
	}
	return domain.DeliveryDraft{
		ID: deliveryID, RouteID: route.Definition.ID, RouteRevision: route.Definition.Revision,
		ExternalSystemID: op.ExternalSystem.ID, APIOperationID: op.Definition.ID,
		APIOperationRevision: op.Definition.Revision, Snapshot: snapshot,
		Status: domain.StatusAccepted, MaxAttempts: maxAttempts(i.config, op),
	}, nil
}

func fingerprintEvent(event domain.EventInput) (string, error) {
	value := struct {
		EventID    string         `json:"event_id"`
		EventType  string         `json:"event_type"`
		OccurredAt string         `json:"occurred_at"`
		RoutingKey string         `json:"routing_key"`
		Payload    map[string]any `json:"payload"`
	}{event.EventID, event.EventType, event.OccurredAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"), event.RoutingKey, event.Payload}
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

func maxAttempts(runtime *config.Runtime, op *config.RuntimeOperation) int {
	if op.Definition.MaxAttempts > 0 {
		return op.Definition.MaxAttempts
	}
	return runtime.File.Worker.MaxAttempts
}

func truncate(value string, limit int) string {
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\r", " "), "\n", " ")
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
