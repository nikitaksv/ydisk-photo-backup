package main

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// A bounded experiment on normal backup traffic, never synthetic traffic.
var tuneIntervals = []int64{500, 300, 200, 150, 100, 80, 60, 50, 40, 30, 20, 10, 0}

type RateTrial struct {
	IntervalMS int64   `json:"interval_ms"`
	Seconds    float64 `json:"seconds"`
	Requests   int64   `json:"requests"`
	Successful int64   `json:"successful"`
	RPS        float64 `json:"rps"`
	Limited    bool    `json:"limited"`
}

type RateTuneStatus struct {
	State            string      `json:"state,omitempty"`
	StepSeconds      int         `json:"step_seconds"`
	LastGoodMS       int64       `json:"last_good_ms"`
	LastGoodVerified bool        `json:"last_good_verified"`
	TestIntervalMS   int64       `json:"test_interval_ms"`
	ElapsedSeconds   float64     `json:"elapsed_seconds"`
	StepRequests     int64       `json:"step_requests"`
	StepRPS          float64     `json:"step_rps"`
	BoundaryMS       *int64      `json:"boundary_ms,omitempty"`
	Trials           []RateTrial `json:"trials,omitempty"`
}

func adaptiveInterval(d time.Duration) bool {
	return d >= 0 && d <= 5*time.Second && d%time.Millisecond == 0
}

func (c *Client) tuneLocked(now time.Time) RateTuneStatus {
	t := c.tune
	t.Trials = append([]RateTrial(nil), t.Trials...)
	if t.State == "probing" && !c.tuneStarted.IsZero() {
		t.ElapsedSeconds = now.Sub(c.tuneStarted).Seconds()
		t.StepRequests = c.requestsStarted - c.tuneRequests
		if t.ElapsedSeconds >= 1 {
			t.StepRPS = float64(t.StepRequests) / t.ElapsedSeconds
		}
	}
	return t
}

func (c *Client) startRateTune() error {
	c.mu.Lock()
	if c.tune.State == "probing" {
		c.mu.Unlock()
		return nil
	}
	// Observe the current floor first. With no proven stage yet, retreat to
	// the next slower configured step (at least 200 ms for the 100 ms probe).
	start := c.baseInterval
	if c.configuredInterval > 0 {
		start = c.configuredInterval
	}
	if !adaptiveInterval(start) {
		c.mu.Unlock()
		return errors.New("неверный начальный интервал")
	}
	safe := slowerTuneInterval(start.Milliseconds())
	c.tune = RateTuneStatus{State: "probing", StepSeconds: 90, LastGoodMS: safe, TestIntervalMS: start.Milliseconds()}
	c.tuneStarted = time.Now()
	c.tuneRequests, c.tuneSuccesses = c.requestsStarted, c.requestsSucceeded
	c.mu.Unlock()
	return c.saveRate()
}

func slowerTuneInterval(ms int64) int64 {
	for i := len(tuneIntervals) - 1; i >= 0; i-- {
		if tuneIntervals[i] > ms {
			return tuneIntervals[i]
		}
	}
	return 5000
}

func (c *Client) stopRateTune() error {
	c.mu.Lock()
	c.tune.State = "stopped"
	c.mu.Unlock()
	return c.saveRate()
}

// Called under c.mu, before releasing a 429 response. In-flight responses
// share one pause and must not cause another step back in that same episode.
func (c *Client) rollbackTuneLocked(now time.Time, firstEpisode bool) (string, bool) {
	if c.tune.State == "zero_reached" {
		c.tune.LastGoodMS = slowerTuneInterval(0)
		c.tune.TestIntervalMS = 0
		c.tune.State = "probing"
	}
	if c.tune.State != "probing" && c.tune.State != "bounded" {
		return "", false
	}
	if c.tune.State == "bounded" && !firstEpisode {
		return "", true
	}
	if c.tune.State == "probing" {
		trial := c.currentTrialLocked(now)
		trial.Limited = true
		c.tune.Trials = append(c.tune.Trials, trial)
		ms := c.tune.TestIntervalMS
		c.tune.BoundaryMS = &ms
	} else {
		// The previously observed stage can also be too fast for a changing
		// quota. Retreat again; never resume the experiment automatically.
		c.tune.LastGoodMS = slowerTuneInterval(c.tune.LastGoodMS)
		c.tune.LastGoodVerified = false
	}
	c.tune.State = "bounded"
	d := time.Duration(c.tune.LastGoodMS) * time.Millisecond
	c.configuredInterval, c.baseInterval, c.fallbackInterval = d, d, d
	if c.Interval < d {
		c.Interval = d
	}
	c.fastFallback = true
	return fmt.Sprintf("Подбор RPS: получен HTTP 429 на %d мс. Возврат к %d мс; общая пауза сохраняется. Повторного разгона не будет.", c.tune.TestIntervalMS, c.tune.LastGoodMS), true
}

func (c *Client) currentTrialLocked(now time.Time) RateTrial {
	secs := now.Sub(c.tuneStarted).Seconds()
	t := RateTrial{IntervalMS: c.tune.TestIntervalMS, Seconds: secs, Requests: c.requestsStarted - c.tuneRequests, Successful: c.requestsSucceeded - c.tuneSuccesses}
	if secs > 0 {
		t.RPS = float64(t.Requests) / secs
	}
	return t
}

func (c *Client) advanceRateTune(now time.Time) error {
	c.mu.Lock()
	if c.tune.State != "probing" {
		c.mu.Unlock()
		return nil
	}
	// Pauses and slower recovery are not evidence that the requested stage
	// is sustainable. Restart its observation once recovery is complete.
	if c.cooldown.After(now) || c.Interval != c.baseInterval || c.backoffLevel > 0 {
		c.tuneStarted = now
		c.tuneRequests = c.requestsStarted
		c.tuneSuccesses = c.requestsSucceeded
		c.mu.Unlock()
		return nil
	}
	trial := c.currentTrialLocked(now)
	if trial.Seconds < float64(c.tune.StepSeconds) || trial.Requests < 100 || trial.Successful < 10 {
		c.mu.Unlock()
		return nil
	}
	c.tune.Trials = append(c.tune.Trials, trial)
	c.tune.LastGoodMS, c.tune.LastGoodVerified = trial.IntervalMS, true
	next := int64(-1)
	for _, ms := range tuneIntervals {
		if ms < trial.IntervalMS {
			next = ms
			break
		}
	}
	message := fmt.Sprintf("Подбор RPS: %d мс, %.2f RPS, %d запросов за %.0f с без 429.", trial.IntervalMS, trial.RPS, trial.Requests, trial.Seconds)
	if next < 0 {
		c.tune.State = "zero_reached"
		message += " Дополнительная задержка снята; параллельность загрузок сохраняется."
	} else {
		d := time.Duration(next) * time.Millisecond
		c.configuredInterval, c.baseInterval, c.Interval = d, d, d
		c.next = now.Add(d)
		c.tune.TestIntervalMS = next
		c.tuneStarted = now
		c.tuneRequests = c.requestsStarted
		c.tuneSuccesses = c.requestsSucceeded
		message += fmt.Sprintf(" Следующая ступень: %d мс.", next)
	}
	c.mu.Unlock()
	if e := c.saveRate(); e != nil {
		return e
	}
	if c.Log != nil {
		c.Log(message)
	}
	return nil
}

func (c *Client) runRateTune(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if e := c.advanceRateTune(now); e != nil {
				c.stopRateTune()
				if c.Log != nil {
					c.Log("Подбор RPS остановлен: не удалось сохранить состояние.")
				}
				return
			}
		}
	}
}
