package domain

import (
	"encoding/json"
	"time"
)

const (
	StatusAccepted   = "ACCEPTED"
	StatusDelivering = "DELIVERING"
	StatusRetryWait  = "RETRY_WAIT"
	StatusSucceeded  = "SUCCEEDED"
	StatusDeadLetter = "DEAD_LETTER"
)

type EventInput struct {
	EventID    string          `json:"event_id"`
	EventType  string          `json:"event_type"`
	OccurredAt time.Time       `json:"occurred_at"`
	RoutingKey string          `json:"routing_key"`
	Payload    map[string]any  `json:"payload"`
	RawPayload json.RawMessage `json:"-"`
}

type RequestSnapshot struct {
	URL                  string            `json:"url"`
	Method               string            `json:"method"`
	Headers              map[string]string `json:"headers"`
	SecretHeaders        map[string]string `json:"secret_headers"`
	Body                 string            `json:"body"`
	BodyEncoding         string            `json:"body_encoding"`
	TimeoutMS            int               `json:"timeout_ms"`
	RetryDelaysMS        []int64           `json:"retry_delays_ms"`
	IdempotencyHeader    string            `json:"idempotency_header,omitempty"`
	EventID              string            `json:"event_id"`
	ExternalSystemID     string            `json:"external_system_id"`
	APIOperationID       string            `json:"api_operation_id"`
	APIOperationRevision int               `json:"api_operation_revision"`
	RouteID              string            `json:"route_id"`
	RouteRevision        int               `json:"route_revision"`
	MaxConcurrency       int               `json:"max_concurrency"`
	RateLimitPerSecond   int               `json:"rate_limit_per_second"`
}

type DeliveryDraft struct {
	ID                   string
	RouteID              string
	RouteRevision        int
	ExternalSystemID     string
	APIOperationID       string
	APIOperationRevision int
	Snapshot             RequestSnapshot
	Status               string
	MaxAttempts          int
	LastErrorClass       string
	LastErrorSummary     string
}

type Delivery struct {
	ID                   string          `json:"id"`
	EventPK              string          `json:"event_pk"`
	RouteID              string          `json:"route_id"`
	RouteRevision        int             `json:"route_revision"`
	ExternalSystemID     string          `json:"external_system_id"`
	APIOperationID       string          `json:"api_operation_id"`
	APIOperationRevision int             `json:"api_operation_revision"`
	Snapshot             RequestSnapshot `json:"-"`
	Status               string          `json:"status"`
	Round                int             `json:"round"`
	AttemptCount         int             `json:"attempt_count"`
	MaxAttempts          int             `json:"max_attempts"`
	NextAttemptAt        *time.Time      `json:"next_attempt_at,omitempty"`
	LeaseOwner           string          `json:"-"`
	LastErrorClass       string          `json:"last_error_class,omitempty"`
	LastErrorSummary     string          `json:"last_error_summary,omitempty"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

type EventRecord struct {
	ID            string         `json:"id"`
	SourceSystem  string         `json:"source_system"`
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	OccurredAt    time.Time      `json:"occurred_at"`
	RoutingKey    string         `json:"routing_key"`
	Payload       map[string]any `json:"payload,omitempty"`
	Fingerprint   string         `json:"-"`
	ConfigVersion string         `json:"config_version"`
	Duplicate     bool           `json:"duplicate"`
	CreatedAt     time.Time      `json:"created_at"`
	Deliveries    []Delivery     `json:"deliveries"`
}

type AttemptResult struct {
	DeliveryID   string
	Round        int
	AttemptNo    int
	LeaseOwner   string
	StartedAt    time.Time
	FinishedAt   time.Time
	HTTPStatus   *int
	ResultClass  string
	ErrorSummary string
	FinalStatus  string
	NextAttempt  *time.Time
}

type DeliveryAttempt struct {
	Round        int       `json:"round"`
	AttemptNo    int       `json:"attempt_no"`
	StartedAt    time.Time `json:"started_at"`
	FinishedAt   time.Time `json:"finished_at"`
	DurationMS   int64     `json:"duration_ms"`
	HTTPStatus   *int      `json:"http_status,omitempty"`
	ResultClass  string    `json:"result_class"`
	ErrorSummary string    `json:"error_summary,omitempty"`
}
