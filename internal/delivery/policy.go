package delivery

import (
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

func Classify(httpStatus int, transportErr error) (class string, retryable bool) {
	if transportErr != nil {
		var permanent interface{ Permanent() bool }
		if errors.As(transportErr, &permanent) && permanent.Permanent() {
			return "CONFIG_ERROR", false
		}
		return "TRANSPORT_ERROR", true
	}
	switch {
	case httpStatus >= 200 && httpStatus < 300:
		return "SUCCEEDED", false
	case httpStatus == http.StatusRequestTimeout:
		return "HTTP_408", true
	case httpStatus == http.StatusTooManyRequests:
		return "HTTP_429", true
	case httpStatus >= 500:
		return "HTTP_5XX", true
	case httpStatus >= 300 && httpStatus < 400:
		return "HTTP_3XX", false
	default:
		return "HTTP_4XX", false
	}
}

func NextDelay(delaysMS []int64, attemptNo int, retryAfter string, now time.Time) time.Duration {
	index := attemptNo - 1
	var base time.Duration
	if index >= 0 && index < len(delaysMS) {
		base = time.Duration(delaysMS[index]) * time.Millisecond
	}
	if suggested, ok := parseRetryAfter(retryAfter, now); ok {
		if suggested < time.Second {
			suggested = time.Second
		}
		if suggested > 24*time.Hour {
			suggested = 24 * time.Hour
		}
		base = suggested
	}
	if base <= 0 {
		base = time.Minute
	}
	// Jitter by ±15% to prevent synchronized retry storms.
	factor := 0.85 + rand.Float64()*0.30
	return time.Duration(float64(base) * factor)
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		return time.Duration(seconds) * time.Second, true
	}
	if date, err := http.ParseTime(value); err == nil {
		d := date.Sub(now)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}
