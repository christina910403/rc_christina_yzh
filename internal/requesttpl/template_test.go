package requesttpl

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRenderDifferentHeadersAndBody(t *testing.T) {
	compiled, err := Compile("inventory", Definition{
		Headers:      map[string]string{"X-Event-ID": "{{ .event_id }}"},
		Query:        map[string]string{"source": "{{ .routing_key }}"},
		BodyEncoding: "application/json",
		Body:         `{"sku":{{ json .payload.sku }},"quantity_delta":{{ .payload.quantity | negate }}}`,
	})
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := compiled.Render(EventContext{
		EventID: "evt-1", EventType: "commerce.order.paid.v1", OccurredAt: time.Now(), RoutingKey: "default",
		Payload: map[string]any{"sku": "SKU-1", "quantity": json.Number("2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if rendered.Headers["X-Event-Id"] != "evt-1" {
		t.Fatalf("unexpected headers: %#v", rendered.Headers)
	}
	if rendered.Query.Get("source") != "default" {
		t.Fatalf("unexpected query: %s", rendered.Query.Encode())
	}
	if rendered.Body != `{"quantity_delta":-2,"sku":"SKU-1"}` {
		t.Fatalf("unexpected body: %s", rendered.Body)
	}
}

func TestRenderRejectsCRLFHeader(t *testing.T) {
	compiled, err := Compile("bad", Definition{Headers: map[string]string{"X-Test": "{{ .payload.value }}"}, Body: `{}`})
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Render(EventContext{Payload: map[string]any{"value": "ok\r\nInjected: yes"}})
	if err == nil {
		t.Fatal("expected CR/LF validation error")
	}
}

func TestRenderRejectsInvalidJSON(t *testing.T) {
	compiled, err := Compile("bad-json", Definition{Body: `{"missing":{{ .payload.value }}}`})
	if err != nil {
		t.Fatal(err)
	}
	_, err = compiled.Render(EventContext{Payload: map[string]any{"value": "not-json"}})
	if err == nil {
		t.Fatal("expected invalid JSON error")
	}
}
