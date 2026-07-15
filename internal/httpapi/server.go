package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/christina910403/rc_christina_yzh/internal/config"
	"github.com/christina910403/rc_christina_yzh/internal/domain"
	"github.com/christina910403/rc_christina_yzh/internal/service"
	"github.com/christina910403/rc_christina_yzh/internal/storage"
)

type Ingestor interface {
	Accept(context.Context, string, domain.EventInput) (domain.EventRecord, error)
	Replay(context.Context, string, string, string, bool) (domain.Delivery, error)
}

type Store interface {
	Ping(context.Context) error
	GetEvent(context.Context, string) (domain.EventRecord, error)
	GetDelivery(context.Context, string) (domain.Delivery, error)
	ListAttempts(context.Context, string) ([]domain.DeliveryAttempt, error)
	ListDeadLetters(context.Context, int) ([]domain.Delivery, error)
}

type Server struct {
	runtime  *config.Runtime
	ingestor Ingestor
	store    Store
	logger   *slog.Logger
	mux      *http.ServeMux
}

func New(runtime *config.Runtime, ingestor Ingestor, store Store, logger *slog.Logger) *Server {
	s := &Server{runtime: runtime, ingestor: ingestor, store: store, logger: logger, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler { return s.logging(s.mux) }

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	s.mux.HandleFunc("GET /health/ready", s.ready)
	s.mux.HandleFunc("POST /api/v1/events", s.authSource(s.acceptEvent))
	s.mux.HandleFunc("GET /api/v1/events/{id}", s.authSource(s.getEvent))
	s.mux.HandleFunc("GET /api/v1/deliveries/{id}", s.authSource(s.getDelivery))
	s.mux.HandleFunc("GET /api/v1/deliveries/{id}/attempts", s.authSource(s.getAttempts))
	s.mux.HandleFunc("GET /ops/v1/dead-letters", s.authOps(s.deadLetters))
	s.mux.HandleFunc("POST /ops/v1/deliveries/{id}/replays", s.authOps(s.replay))
}

func (s *Server) acceptEvent(w http.ResponseWriter, r *http.Request, source string) {
	r.Body = http.MaxBytesReader(w, r.Body, s.runtime.File.Server.MaxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var request struct {
		EventID    string          `json:"event_id"`
		EventType  string          `json:"event_type"`
		OccurredAt string          `json:"occurred_at"`
		RoutingKey string          `json:"routing_key"`
		Payload    json.RawMessage `json:"payload"`
	}
	if err := decoder.Decode(&request); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			writeError(w, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request exceeds configured limit")
			return
		}
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", err.Error())
		return
	}
	if err := ensureEOF(decoder); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "request must contain exactly one JSON object")
		return
	}
	if request.EventID == "" || request.EventType == "" || request.OccurredAt == "" || len(request.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "event_id, event_type, occurred_at and payload are required")
		return
	}
	occurredAt, err := time.Parse(time.RFC3339, request.OccurredAt)
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_OCCURRED_AT", "occurred_at must be RFC3339")
		return
	}
	var payload map[string]any
	payloadDecoder := json.NewDecoder(strings.NewReader(string(request.Payload)))
	payloadDecoder.UseNumber()
	if err := payloadDecoder.Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PAYLOAD", "payload must be a JSON object")
		return
	}
	if err := ensureEOF(payloadDecoder); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_PAYLOAD", "payload must contain exactly one JSON object")
		return
	}
	routingKey := request.RoutingKey
	if routingKey == "" {
		routingKey = "default"
	}
	record, err := s.ingestor.Accept(r.Context(), source, domain.EventInput{
		EventID: request.EventID, EventType: request.EventType, OccurredAt: occurredAt,
		RoutingKey: routingKey, Payload: payload, RawPayload: request.Payload,
	})
	if err != nil {
		s.writeAcceptError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, eventResponse(record))
}

func (s *Server) getEvent(w http.ResponseWriter, r *http.Request, source string) {
	record, err := s.store.GetEvent(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	if record.SourceSystem != source {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "event not found")
		return
	}
	writeJSON(w, http.StatusOK, eventResponse(record))
}

func (s *Server) getDelivery(w http.ResponseWriter, r *http.Request, source string) {
	delivery, err := s.store.GetDelivery(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	record, err := s.store.GetEvent(r.Context(), delivery.EventPK)
	if err != nil || record.SourceSystem != source {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "delivery not found")
		return
	}
	writeJSON(w, http.StatusOK, delivery)
}

func (s *Server) getAttempts(w http.ResponseWriter, r *http.Request, source string) {
	delivery, err := s.store.GetDelivery(r.Context(), r.PathValue("id"))
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	record, err := s.store.GetEvent(r.Context(), delivery.EventPK)
	if err != nil || record.SourceSystem != source {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "delivery not found")
		return
	}
	attempts, err := s.store.ListAttempts(r.Context(), delivery.ID)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"delivery_id": delivery.ID, "attempts": attempts})
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request, _ string) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	items, err := s.store.ListDeadLetters(r.Context(), limit)
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request, operator string) {
	var request struct {
		Reason           string `json:"reason"`
		UseCurrentConfig bool   `json:"use_current_config"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8*1024)).Decode(&request); err != nil || strings.TrimSpace(request.Reason) == "" {
		writeError(w, http.StatusBadRequest, "INVALID_REQUEST", "reason is required")
		return
	}
	delivery, err := s.ingestor.Replay(r.Context(), r.PathValue("id"), operator, strings.TrimSpace(request.Reason), request.UseCurrentConfig)
	if errors.Is(err, storage.ErrInvalidState) {
		writeError(w, http.StatusConflict, "INVALID_STATE", "only dead-letter deliveries can be replayed")
		return
	}
	if errors.Is(err, service.ErrRouteNotFound) {
		writeError(w, http.StatusUnprocessableEntity, "ROUTE_NOT_AVAILABLE", err.Error())
		return
	}
	if err != nil {
		s.writeStoreError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, delivery)
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "NOT_READY", "postgres is unavailable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "config_version": s.runtime.Version})
}

func (s *Server) authSource(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		source, ok := s.runtime.Authenticate(r.Header.Get("X-API-Key"))
		if !ok {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid API key")
			return
		}
		next(w, r, source)
	}
}

func (s *Server) authOps(next func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-Ops-Key")
		if !s.runtime.AuthenticateOps(key) {
			writeError(w, http.StatusUnauthorized, "UNAUTHORIZED", "invalid operations key")
			return
		}
		operator := r.Header.Get("X-Operator")
		if operator == "" {
			writeError(w, http.StatusBadRequest, "OPERATOR_REQUIRED", "X-Operator is required")
			return
		}
		next(w, r, operator)
	}
}

func (s *Server) writeAcceptError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrUnknownEvent):
		writeError(w, http.StatusNotFound, "EVENT_TYPE_NOT_FOUND", err.Error())
	case errors.Is(err, service.ErrForbiddenEvent):
		writeError(w, http.StatusForbidden, "EVENT_FORBIDDEN", err.Error())
	case errors.Is(err, service.ErrInvalidPayload):
		writeError(w, http.StatusUnprocessableEntity, "EVENT_SCHEMA_VIOLATION", err.Error())
	case errors.Is(err, service.ErrNoRoute):
		writeError(w, http.StatusUnprocessableEntity, "NO_ACTIVE_ROUTE", err.Error())
	case errors.Is(err, storage.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_CONFLICT", err.Error())
	default:
		s.logger.Error("accept event", "error", err)
		writeError(w, http.StatusServiceUnavailable, "PERSISTENCE_UNAVAILABLE", "event was not accepted")
	}
}

func (s *Server) writeStoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrNotFound) {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	s.logger.Error("storage operation", "error", err)
	writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "storage operation failed")
}

func eventResponse(record domain.EventRecord) map[string]any {
	return map[string]any{
		"id": record.ID, "event_id": record.EventID, "event_type": record.EventType,
		"status": aggregateStatus(record.Deliveries), "duplicate": record.Duplicate,
		"matched_routes": len(record.Deliveries), "created_at": record.CreatedAt, "deliveries": record.Deliveries,
	}
}

func aggregateStatus(items []domain.Delivery) string {
	if len(items) == 0 {
		return "NO_SUBSCRIBER"
	}
	succeeded, dead := 0, 0
	for _, item := range items {
		if item.Status == domain.StatusSucceeded {
			succeeded++
		}
		if item.Status == domain.StatusDeadLetter {
			dead++
		}
	}
	if succeeded == len(items) {
		return domain.StatusSucceeded
	}
	if dead == len(items) {
		return domain.StatusDeadLetter
	}
	if succeeded > 0 || dead > 0 {
		return "PARTIAL"
	}
	return "PROCESSING"
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Info("http request", "method", r.Method, "path", r.URL.Path, "duration_ms", time.Since(started).Milliseconds())
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func ensureEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing JSON value")
	}
	return nil
}
