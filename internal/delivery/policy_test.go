package delivery

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		class  string
		retry  bool
	}{
		{"success", http.StatusNoContent, nil, "SUCCEEDED", false},
		{"rate-limit", http.StatusTooManyRequests, nil, "HTTP_429", true},
		{"server-error", http.StatusServiceUnavailable, nil, "HTTP_5XX", true},
		{"bad-request", http.StatusBadRequest, nil, "HTTP_4XX", false},
		{"transport", 0, errors.New("timeout"), "TRANSPORT_ERROR", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, retry := Classify(test.status, test.err)
			if class != test.class || retry != test.retry {
				t.Fatalf("got %s/%v", class, retry)
			}
		})
	}
}

func TestNextDelayUsesBoundedRetryAfter(t *testing.T) {
	delay := NextDelay([]int64{60000}, 1, "999999", time.Now())
	if delay < 20*time.Hour || delay > 28*time.Hour {
		t.Fatalf("unexpected bounded jittered delay: %v", delay)
	}
}
