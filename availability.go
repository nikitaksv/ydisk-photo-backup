package main

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

// A probe timeout says nothing about permanent availability. It moves this
// pass to the next quality while preserving the reason for a later check.
type ProbeTimeoutError struct{ Stage string }

func (e *ProbeTimeoutError) Error() string {
	return "таймаут проверки " + e.Stage + "; доступность не подтверждена"
}
func (e *ProbeTimeoutError) Timeout() bool { return true }
func isProbeTimeout(e error) bool          { var probe *ProbeTimeoutError; return errors.As(e, &probe) }
func timeoutError(e error) bool {
	var network net.Error
	return isProbeTimeout(e) || errors.As(e, &network) && network.Timeout()
}
func durationOr(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}

// Only time blocked network reads. Hashing, disk writes and time in the shared
// rate limiter are outside this watchdog. One timer is reused per response.
type progressBody struct {
	body                           io.ReadCloser
	cancel                         context.CancelFunc
	budgetExpired                  func() bool
	mu                             sync.Mutex
	timer                          *time.Timer
	firstWait, idleWait            time.Duration
	deadline                       time.Time
	seen, reading, expired, closed bool
}

func (b *progressBody) expire() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || !b.reading {
		return
	}
	// A previous timer callback may have been queued before the next Read.
	if remaining := time.Until(b.deadline); remaining > 0 {
		b.timer.Reset(remaining)
		return
	}
	b.expired = true
	b.cancel()
}
func (b *progressBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}
	if b.expired {
		b.mu.Unlock()
		return 0, &ProbeTimeoutError{Stage: "поступления данных"}
	}
	wait := b.firstWait
	if b.seen {
		wait = b.idleWait
	}
	b.reading = true
	b.deadline = time.Now().Add(wait)
	if b.timer == nil {
		b.timer = time.AfterFunc(wait, b.expire)
	} else {
		b.timer.Reset(wait)
	}
	b.mu.Unlock()
	n, e := b.body.Read(p)
	b.mu.Lock()
	b.reading = false
	b.timer.Stop()
	if n > 0 {
		b.seen = true
	}
	if b.expired {
		e = &ProbeTimeoutError{Stage: "поступления данных"}
	} else if e != nil && e != io.EOF && b.budgetExpired != nil && b.budgetExpired() {
		e = &ProbeTimeoutError{Stage: "полного получения ответа"}
	}
	b.mu.Unlock()
	return n, e
}
func (b *progressBody) Close() error {
	b.mu.Lock()
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
	}
	b.mu.Unlock()
	b.cancel()
	return b.body.Close()
}
