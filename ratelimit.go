package main

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RateStatus struct {
	LimitedUntil         string         `json:"limited_until,omitempty"`
	IntervalMS           int64          `json:"interval_ms"`
	BaseIntervalMS       int64          `json:"base_interval_ms,omitempty"`
	Events               int            `json:"events"`
	Level                int            `json:"backoff_level"`
	ConfiguredIntervalMS int64          `json:"configured_interval_ms,omitempty"`
	FallbackIntervalMS   int64          `json:"fallback_interval_ms,omitempty"`
	Fast429Since         string         `json:"fast_429_since,omitempty"`
	Fast429Episodes      int            `json:"fast_429_episodes,omitempty"`
	FastFallback         bool           `json:"fast_fallback,omitempty"`
	Requests             int64          `json:"requests"`
	SuccessfulRequests   int64          `json:"successful_requests"`
	MeasurementSeconds   float64        `json:"measurement_seconds"`
	ObservedRPS          float64        `json:"observed_rps"`
	Tune                 RateTuneStatus `json:"tune"`
}

func retryAfterDuration(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if n, e := strconv.ParseInt(value, 10, 64); e == nil && n >= 0 && n <= int64((1<<63-1)/int64(time.Second)) {
		return time.Duration(n) * time.Second
	}
	if t, e := http.ParseTime(value); e == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
func (c *Client) rateLocked() RateStatus {
	s := RateStatus{IntervalMS: c.Interval.Milliseconds(), BaseIntervalMS: c.baseInterval.Milliseconds(), Events: c.limitEvents, Level: c.backoffLevel}
	s.ConfiguredIntervalMS = c.configuredInterval.Milliseconds()
	s.FallbackIntervalMS = c.fallbackInterval.Milliseconds()
	s.Fast429Episodes, s.FastFallback = c.fast429Episodes, c.fastFallback
	s.Tune = c.tuneLocked(time.Now())
	if !c.fast429Since.IsZero() {
		s.Fast429Since = c.fast429Since.UTC().Format(time.RFC3339Nano)
	}
	s.Requests, s.SuccessfulRequests = c.requestsStarted, c.requestsSucceeded
	if !c.measurementStart.IsZero() {
		s.MeasurementSeconds = time.Since(c.measurementStart).Seconds()
		if s.MeasurementSeconds >= 1 {
			s.ObservedRPS = float64(s.Requests) / s.MeasurementSeconds
		}
	}
	if c.cooldown.After(time.Now()) {
		s.LimitedUntil = c.cooldown.UTC().Format(time.RFC3339Nano)
	}
	return s
}
func (c *Client) rateStatus() RateStatus { c.mu.Lock(); defer c.mu.Unlock(); return c.rateLocked() }
func (c *Client) saveRate() error {
	if c.Checkpoint == nil {
		return nil
	}
	c.checkpointMu.Lock()
	defer c.checkpointMu.Unlock()
	// Persist the latest state rather than a stale callback snapshot.
	if e := c.Checkpoint(c.rateStatus()); e != nil {
		return &LocalError{e}
	}
	return nil
}
func (c *Client) restoreRate(s RateStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()
	configured := time.Duration(s.ConfiguredIntervalMS) * time.Millisecond
	if validRequestInterval(configured) || s.Tune.State != "" && adaptiveInterval(configured) {
		c.configuredInterval = time.Duration(s.ConfiguredIntervalMS) * time.Millisecond
		c.Interval = c.configuredInterval
	}
	c.baseInterval = c.Interval
	c.fallbackInterval = defaultFallbackInterval(c.Interval)
	if s.FallbackIntervalMS >= 100 && s.FallbackIntervalMS <= 5000 && time.Duration(s.FallbackIntervalMS)*time.Millisecond >= c.Interval {
		c.fallbackInterval = time.Duration(s.FallbackIntervalMS) * time.Millisecond
	}
	c.fast429Since, _ = time.Parse(time.RFC3339Nano, s.Fast429Since)
	if s.Fast429Episodes >= 0 && s.Fast429Episodes <= 20 {
		c.fast429Episodes = s.Fast429Episodes
	}
	c.fastFallback = s.FastFallback
	c.cooldown, _ = time.Parse(time.RFC3339Nano, s.LimitedUntil)
	previousBase := s.BaseIntervalMS
	if previousBase == 0 {
		previousBase = 3000 // Baseline used by earlier releases.
	}
	fullyRecovered := s.Level == 0 && !c.cooldown.After(time.Now()) && s.IntervalMS <= previousBase
	if !fullyRecovered && s.IntervalMS > c.Interval.Milliseconds() && s.IntervalMS <= 30000 {
		c.Interval = time.Duration(s.IntervalMS) * time.Millisecond
	}
	c.backoffLevel = s.Level
	if c.backoffLevel < 0 {
		c.backoffLevel = 0
	}
	if c.backoffLevel > 20 {
		c.backoffLevel = 20
	}
	c.limitEvents = s.Events
	c.tune = s.Tune
	c.tuneStarted = time.Now()
	c.tuneRequests, c.tuneSuccesses = c.requestsStarted, c.requestsSucceeded
}

func validRequestInterval(interval time.Duration) bool {
	return interval >= 100*time.Millisecond && interval <= 5*time.Second
}

func defaultFallbackInterval(interval time.Duration) time.Duration {
	if interval < 200*time.Millisecond {
		return 200 * time.Millisecond
	}
	return 500 * time.Millisecond
}

// Changing the baseline never clears a server cooldown or a slower backoff.
func (c *Client) configureInterval(interval time.Duration, fallback ...time.Duration) error {
	if !validRequestInterval(interval) {
		return errors.New("интервал запросов: 100..5000 мс")
	}
	target := defaultFallbackInterval(interval)
	if len(fallback) > 1 {
		return errors.New("нужен один интервал возврата")
	}
	if len(fallback) == 1 {
		target = fallback[0]
		if !validRequestInterval(target) || target <= interval {
			return errors.New("интервал возврата должен быть больше исходного, не более 5000 мс")
		}
	}
	c.mu.Lock()
	c.configuredInterval, c.baseInterval = interval, interval
	c.fallbackInterval = target
	if c.backoffLevel == 0 && !c.cooldown.After(time.Now()) {
		c.Interval = interval
		c.next = time.Now().Add(interval)
	} else if c.Interval < interval {
		c.Interval = interval
	}
	c.fast429Since, c.fast429Episodes, c.fastFallback = time.Time{}, 0, false
	c.accepted = 0
	c.tune.State = "stopped"
	c.mu.Unlock()
	return c.saveRate()
}

func (c *Client) recordRequest() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.measurementStart.IsZero() {
		c.measurementStart = time.Now()
	}
	c.requestsStarted++
}
func (c *Client) throttle(header string) error {
	now := time.Now()
	serverDelay := retryAfterDuration(header, now)
	c.mu.Lock()
	if c.Interval <= 0 {
		c.Interval = time.Millisecond
	}
	if c.baseInterval == 0 && c.tune.State == "" {
		c.baseInterval = c.Interval
	}
	firstInEpisode := !c.cooldown.After(now)
	base, max := c.CooldownBase, c.CooldownMax
	if base <= 0 {
		base = 90 * time.Second
	}
	if max <= 0 {
		max = 30 * time.Minute
	}
	delay := base
	if firstInEpisode {
		for i := 0; i < c.backoffLevel && delay < max; i++ {
			if delay > max/2 {
				delay = max
				break
			}
			delay *= 2
		}
		if delay > max {
			delay = max
		}
		if c.backoffLevel < 20 {
			c.backoffLevel++
		}
		c.Interval *= 2
		if c.Interval > 30*time.Second {
			c.Interval = 30 * time.Second
		}
		c.accepted = 0
	} else {
		delay = time.Until(c.cooldown)
	}
	if serverDelay > delay {
		delay = serverDelay
	}
	tuneMessage, tuningProtection := c.rollbackTuneLocked(now, firstInEpisode)
	fallback := false
	target := c.fallbackInterval
	if target <= 0 {
		target = defaultFallbackInterval(c.configuredInterval)
	}
	if !tuningProtection && c.configuredInterval > 0 && c.baseInterval < target {
		if firstInEpisode {
			if c.fast429Since.IsZero() || now.Sub(c.fast429Since) > 10*time.Minute {
				c.fast429Since, c.fast429Episodes = now, 0
			}
			c.fast429Episodes++
		}
		if c.fast429Episodes >= 2 || delay >= 3*time.Minute {
			c.configuredInterval, c.baseInterval = target, target
			if c.Interval < c.baseInterval {
				c.Interval = c.baseInterval
			}
			c.fastFallback, fallback = true, true
		}
	}
	until := now.Add(delay)
	if until.After(c.cooldown) {
		c.cooldown = until
	}
	c.limitEvents++
	s := c.rateLocked()
	c.mu.Unlock()
	if e := c.saveRate(); e != nil {
		return e
	}
	if c.Log != nil && tuneMessage != "" {
		c.Log(tuneMessage)
	}
	if c.Log != nil && fallback {
		c.Log(fmt.Sprintf("Проба ускорения остановлена: повторные HTTP 429 или пауза от 3 минут. Базовый интервал возвращён к %.1f с; пауза Яндекса сохраняется.", float64(target)/float64(time.Second)))
	}
	if c.Log != nil && firstInEpisode {
		c.Log(fmt.Sprintf("HTTP 429: общая пауза до %s; интервал новых запросов %.1f с. Файлы остаются в очереди.", s.LimitedUntil, float64(s.IntervalMS)/1000))
	}
	return nil
}
func (c *Client) acceptedRequest() error {
	c.mu.Lock()
	c.requestsSucceeded++
	c.accepted++
	changed := false
	if c.accepted >= 20 {
		c.accepted = 0
		if c.Interval > c.baseInterval && (c.baseInterval > 0 || c.tune.State != "") {
			c.Interval = c.Interval * 3 / 4
			if c.Interval < c.baseInterval {
				c.Interval = c.baseInterval
			}
			changed = true
		}
		if c.backoffLevel > 0 {
			c.backoffLevel--
			changed = true
		}
	}
	c.mu.Unlock()
	if changed {
		return c.saveRate()
	}
	return nil
}
