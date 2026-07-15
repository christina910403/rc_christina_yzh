package worker

import (
	"bytes"
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/christina910403/rc_christina_yzh/internal/delivery"
	"github.com/christina910403/rc_christina_yzh/internal/domain"
)

type Store interface {
	ClaimDeliveries(context.Context, string, int, time.Duration) ([]domain.Delivery, error)
	FinishAttempt(context.Context, domain.AttemptResult) error
}

type Config struct {
	OwnerID             string
	BatchSize           int
	PollInterval        time.Duration
	LeaseDuration       time.Duration
	AllowPrivateNetwork bool
}

type Worker struct {
	store     Store
	config    Config
	transport http.RoundTripper
	logger    *slog.Logger
	gatesMu   sync.Mutex
	gates     map[string]*targetGate
}

func New(store Store, config Config, logger *slog.Logger) *Worker {
	if config.BatchSize <= 0 {
		config.BatchSize = 50
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = time.Minute
	}
	return &Worker{
		store: store, config: config, transport: newTransport(config.AllowPrivateNetwork),
		logger: logger, gates: map[string]*targetGate{},
	}
}

func (w *Worker) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		deliveries, err := w.store.ClaimDeliveries(ctx, w.config.OwnerID, w.config.BatchSize, w.config.LeaseDuration)
		if err != nil {
			w.logger.Error("claim deliveries", "error", err)
			timer.Reset(w.config.PollInterval)
			continue
		}
		if len(deliveries) == 0 {
			timer.Reset(w.config.PollInterval)
			continue
		}
		var wg sync.WaitGroup
		for _, item := range deliveries {
			item := item
			wg.Add(1)
			go func() {
				defer wg.Done()
				w.process(ctx, item)
			}()
		}
		wg.Wait()
		timer.Reset(10 * time.Millisecond)
	}
}

func (w *Worker) process(ctx context.Context, item domain.Delivery) {
	gate := w.gateFor(item.Snapshot)
	if err := gate.acquire(ctx); err != nil {
		return
	}
	defer gate.release()
	started := time.Now().UTC()
	status, retryAfter, callErr := w.send(ctx, item)
	finished := time.Now().UTC()
	class, retryable := delivery.Classify(status, callErr)
	result := domain.AttemptResult{
		DeliveryID: item.ID, Round: item.Round, AttemptNo: item.AttemptCount,
		LeaseOwner: item.LeaseOwner, StartedAt: started, FinishedAt: finished,
		ResultClass: class,
	}
	if status > 0 {
		result.HTTPStatus = &status
	}
	if callErr != nil {
		result.ErrorSummary = sanitize(callErr.Error(), 512)
	}
	if class == "SUCCEEDED" {
		result.FinalStatus = domain.StatusSucceeded
	} else if retryable && item.AttemptCount < item.MaxAttempts {
		result.FinalStatus = domain.StatusRetryWait
		delay := delivery.NextDelay(item.Snapshot.RetryDelaysMS, item.AttemptCount, retryAfter, finished)
		next := finished.Add(delay)
		result.NextAttempt = &next
	} else {
		result.FinalStatus = domain.StatusDeadLetter
		if retryable && item.AttemptCount >= item.MaxAttempts {
			result.ResultClass = "RETRY_EXHAUSTED_" + class
		}
	}
	if err := w.store.FinishAttempt(ctx, result); err != nil {
		w.logger.Error("finish delivery attempt", "delivery_id", item.ID, "error", err)
		return
	}
	w.logger.Info("delivery attempt finished", "delivery_id", item.ID, "route_id", item.RouteID,
		"target", item.ExternalSystemID, "attempt", item.AttemptCount, "status", result.FinalStatus,
		"result_class", result.ResultClass, "http_status", status)
}

func (w *Worker) send(parent context.Context, item domain.Delivery) (int, string, error) {
	timeout := time.Duration(item.Snapshot.TimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, item.Snapshot.Method, item.Snapshot.URL, bytes.NewBufferString(item.Snapshot.Body))
	if err != nil {
		return 0, "", permanentError{err}
	}
	for name, value := range item.Snapshot.Headers {
		req.Header.Set(name, value)
	}
	for name, ref := range item.Snapshot.SecretHeaders {
		value, err := resolveSecret(ref)
		if err != nil {
			return 0, "", permanentError{err}
		}
		if req.Header.Get(name) != "" {
			return 0, "", permanentError{fmt.Errorf("secret header %s conflicts with existing header", name)}
		}
		req.Header.Set(name, value)
	}
	if name := item.Snapshot.IdempotencyHeader; name != "" {
		if req.Header.Get(name) != "" {
			return 0, "", permanentError{fmt.Errorf("idempotency header %s conflicts with existing header", name)}
		}
		req.Header.Set(name, item.Snapshot.EventID)
	}
	client := &http.Client{
		Transport: w.transport, Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	resp, err := client.Do(req)
	if err != nil {
		if permanentTLSError(err) {
			return 0, "", permanentError{err}
		}
		var permanent permanentError
		if errors.As(err, &permanent) {
			return 0, "", permanent
		}
		return 0, "", err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 32*1024))
	return resp.StatusCode, resp.Header.Get("Retry-After"), nil
}

func permanentTLSError(err error) bool {
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalid x509.CertificateInvalidError
	var roots x509.SystemRootsError
	return errors.As(err, &unknownAuthority) || errors.As(err, &hostname) ||
		errors.As(err, &invalid) || errors.As(err, &roots)
}

func (w *Worker) gateFor(snapshot domain.RequestSnapshot) *targetGate {
	w.gatesMu.Lock()
	defer w.gatesMu.Unlock()
	gate := w.gates[snapshot.ExternalSystemID]
	if gate == nil {
		concurrency := snapshot.MaxConcurrency
		if concurrency <= 0 {
			concurrency = 4
		}
		gate = &targetGate{semaphore: make(chan struct{}, concurrency), rate: snapshot.RateLimitPerSecond}
		w.gates[snapshot.ExternalSystemID] = gate
	}
	return gate
}

type targetGate struct {
	semaphore chan struct{}
	rate      int
	mu        sync.Mutex
	next      time.Time
}

func (g *targetGate) acquire(ctx context.Context) error {
	select {
	case g.semaphore <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if g.rate <= 0 {
		return nil
	}
	g.mu.Lock()
	now := time.Now()
	if g.next.Before(now) {
		g.next = now
	}
	wait := time.Until(g.next)
	g.next = g.next.Add(time.Second / time.Duration(g.rate))
	g.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		g.release()
		return ctx.Err()
	}
}

func (g *targetGate) release() { <-g.semaphore }

func newTransport(allowPrivate bool) *http.Transport {
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	return &http.Transport{
		Proxy: nil,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if allowPrivate {
				return dialer.DialContext(ctx, network, address)
			}
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			addresses, err := net.DefaultResolver.LookupIPAddr(ctx, host)
			if err != nil {
				return nil, err
			}
			for _, resolved := range addresses {
				if forbiddenIP(resolved.IP) {
					continue
				}
				return dialer.DialContext(ctx, network, net.JoinHostPort(resolved.IP.String(), port))
			}
			return nil, fmt.Errorf("target %s resolves only to forbidden addresses", host)
		},
		ForceAttemptHTTP2: true, MaxIdleConns: 100, MaxIdleConnsPerHost: 20,
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second, MaxResponseHeaderBytes: 64 * 1024,
	}
}

func forbiddenIP(ip net.IP) bool {
	return ip == nil || ip.IsUnspecified() || ip.IsLoopback() || ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast()
}

func resolveSecret(ref string) (string, error) {
	if !strings.HasPrefix(ref, "env:") {
		return "", fmt.Errorf("unsupported secret reference")
	}
	name := strings.TrimPrefix(ref, "env:")
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("secret environment variable %s is empty", name)
	}
	return value, nil
}

type permanentError struct{ error }

func (permanentError) Permanent() bool { return true }

func sanitize(value string, limit int) string {
	value = strings.ReplaceAll(strings.ReplaceAll(value, "\r", " "), "\n", " ")
	if len(value) > limit {
		return value[:limit]
	}
	return value
}
