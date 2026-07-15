package worker

import (
	"context"
	"crypto/x509"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/christina910403/rc_christina_yzh/internal/domain"
)

type fakeStore struct{ result chan domain.AttemptResult }

func (*fakeStore) ClaimDeliveries(context.Context, string, int, time.Duration) ([]domain.Delivery, error) {
	return nil, nil
}

func TestCertificateVerificationFailureIsPermanent(t *testing.T) {
	if !permanentTLSError(x509.UnknownAuthorityError{}) {
		t.Fatal("certificate verification failure should not be retried")
	}
}
func (f *fakeStore) FinishAttempt(_ context.Context, result domain.AttemptResult) error {
	f.result <- result
	return nil
}

func TestWorkerRetries5xxAndSucceeds2xx(t *testing.T) {
	for _, test := range []struct {
		name     string
		status   int
		attempts int
		want     string
	}{
		{"success", http.StatusOK, 1, domain.StatusSucceeded},
		{"retry", http.StatusServiceUnavailable, 1, domain.StatusRetryWait},
		{"exhausted", http.StatusServiceUnavailable, 7, domain.StatusDeadLetter},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{result: make(chan domain.AttemptResult, 1)}
			w := New(store, Config{OwnerID: "worker-1", AllowPrivateNetwork: true}, slog.Default())
			w.transport = roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: test.status,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(`{"received":true}`)),
				}, nil
			})
			w.process(context.Background(), domain.Delivery{
				ID: "d-1", RouteID: "route", ExternalSystemID: "target", Status: domain.StatusDelivering,
				Round: 1, AttemptCount: test.attempts, MaxAttempts: 7, LeaseOwner: "worker-1",
				Snapshot: domain.RequestSnapshot{URL: "https://vendor.example/api", Method: http.MethodPost, Body: `{}`,
					Headers: map[string]string{"Content-Type": "application/json"}, TimeoutMS: 1000,
					RetryDelaysMS: []int64{1, 1, 1, 1, 1, 1}, ExternalSystemID: "target", MaxConcurrency: 1},
			})
			result := <-store.result
			if result.FinalStatus != test.want {
				t.Fatalf("got %s want %s", result.FinalStatus, test.want)
			}
			if test.want == domain.StatusRetryWait && result.NextAttempt == nil {
				t.Fatal("retry lacks next attempt")
			}
		})
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
