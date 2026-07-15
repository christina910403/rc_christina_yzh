package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/christina910403/rc_christina_yzh/internal/config"
	"github.com/christina910403/rc_christina_yzh/internal/domain"
)

type captureStore struct{ drafts []domain.DeliveryDraft }

func (s *captureStore) AcceptEvent(_ context.Context, record domain.EventRecord, _ string, drafts []domain.DeliveryDraft) (domain.EventRecord, error) {
	s.drafts = drafts
	for _, draft := range drafts {
		record.Deliveries = append(record.Deliveries, domain.Delivery{
			ID: draft.ID, RouteID: draft.RouteID, ExternalSystemID: draft.ExternalSystemID,
			APIOperationID: draft.APIOperationID, Status: draft.Status, Snapshot: draft.Snapshot,
		})
	}
	return record, nil
}

type replayCaptureStore struct {
	captureStore
	event    domain.EventRecord
	delivery domain.Delivery
	snapshot *domain.RequestSnapshot
}

func (s *replayCaptureStore) GetDelivery(context.Context, string) (domain.Delivery, error) {
	return s.delivery, nil
}
func (s *replayCaptureStore) GetEvent(context.Context, string) (domain.EventRecord, error) {
	return s.event, nil
}
func (s *replayCaptureStore) Replay(_ context.Context, _ string, _ string, _ string, snapshot *domain.RequestSnapshot) (domain.Delivery, error) {
	s.snapshot = snapshot
	return s.delivery, nil
}

func TestAcceptFansOutDifferentRequests(t *testing.T) {
	runtime, err := config.Load(filepath.Join("..", "..", "configs", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	store := &captureStore{}
	ingestor := NewIngestor(runtime, store)
	_, err = ingestor.Accept(context.Background(), "order-service", domain.EventInput{
		EventID: "event-1", EventType: "commerce.order.paid.v1", OccurredAt: time.Now(), RoutingKey: "default",
		Payload: map[string]any{
			"order_id": "O-1", "customer_id": "C-1", "sku": "SKU-1",
			"quantity": json.Number("2"), "amount": json.Number("19900"), "currency": "CNY",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(store.drafts) != 2 {
		t.Fatalf("got %d deliveries, want 2", len(store.drafts))
	}
	requests := map[string]domain.RequestSnapshot{}
	for _, draft := range store.drafts {
		if draft.Status != domain.StatusAccepted {
			t.Fatalf("route %s unexpectedly failed: %s", draft.RouteID, draft.LastErrorSummary)
		}
		requests[draft.RouteID] = draft.Snapshot
	}
	inventory := requests["order-paid-to-inventory"]
	crm := requests["order-paid-to-crm"]
	if inventory.URL == crm.URL {
		t.Fatalf("routes should use different API URLs: %s", inventory.URL)
	}
	if inventory.Body == crm.Body {
		t.Fatal("routes should render different bodies")
	}
	if inventory.Headers["X-Correlation-Id"] != "event-1" {
		t.Fatalf("unexpected inventory headers: %#v", inventory.Headers)
	}
	if crm.Headers["X-Source-System"] != "order-service" {
		t.Fatalf("unexpected CRM headers: %#v", crm.Headers)
	}
}

func TestAcceptRejectsSchemaViolation(t *testing.T) {
	runtime, err := config.Load(filepath.Join("..", "..", "configs", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	ingestor := NewIngestor(runtime, &captureStore{})
	_, err = ingestor.Accept(context.Background(), "order-service", domain.EventInput{
		EventID: "event-bad", EventType: "commerce.order.paid.v1", OccurredAt: time.Now(), RoutingKey: "default",
		Payload: map[string]any{"order_id": "O-1"},
	})
	if !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("got %v, want invalid payload", err)
	}
}

func TestReplayCanRenderCurrentRouteRevision(t *testing.T) {
	runtime, err := config.Load(filepath.Join("..", "..", "configs", "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	store := &replayCaptureStore{
		event: domain.EventRecord{ID: "event-pk", EventID: "event-1", EventType: "commerce.order.paid.v1", OccurredAt: time.Now(), RoutingKey: "default",
			Payload: map[string]any{"order_id": "O-1", "customer_id": "C-1", "sku": "SKU-1", "quantity": json.Number("1"), "amount": json.Number("100"), "currency": "CNY"}},
		delivery: domain.Delivery{ID: "delivery-1", EventPK: "event-pk", RouteID: "order-paid-to-inventory", Status: domain.StatusDeadLetter},
	}
	ingestor := NewIngestor(runtime, store)
	if _, err := ingestor.Replay(context.Background(), "delivery-1", "oncall", "fixed mapping", true); err != nil {
		t.Fatal(err)
	}
	if store.snapshot == nil {
		t.Fatal("expected a newly rendered snapshot")
	}
	if store.snapshot.RouteID != "order-paid-to-inventory" || store.snapshot.APIOperationID != "inventory.adjust-stock.v1" {
		t.Fatalf("unexpected snapshot: %#v", store.snapshot)
	}
}
