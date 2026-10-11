package main

import (
	"context"
	"testing"
	"time"
)

func finishTuneStage(c *Client, now time.Time) {
	c.mu.Lock()
	c.tuneStarted = now.Add(-91 * time.Second)
	c.requestsStarted = c.tuneRequests + 500
	c.requestsSucceeded = c.tuneSuccesses + 50
	c.mu.Unlock()
}
func TestTuneAdvancesOnlyWithEnoughRealTraffic(t *testing.T) {
	c := &Client{Interval: 100 * time.Millisecond, baseInterval: 100 * time.Millisecond, configuredInterval: 100 * time.Millisecond}
	if e := c.startRateTune(); e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	c.mu.Lock()
	c.tuneStarted = now.Add(-2 * time.Minute)
	c.mu.Unlock()
	c.advanceRateTune(now)
	if c.rateStatus().IntervalMS != 100 {
		t.Fatal("idle time proved a stage")
	}
	finishTuneStage(c, now)
	if e := c.advanceRateTune(now); e != nil {
		t.Fatal(e)
	}
	s := c.rateStatus()
	if s.IntervalMS != 80 || s.Tune.LastGoodMS != 100 || !s.Tune.LastGoodVerified || len(s.Tune.Trials) != 1 {
		t.Fatal(s)
	}
}
func TestTuneFirst429RetreatsOnceAndSurvivesRestart(t *testing.T) {
	c := &Client{Interval: 100 * time.Millisecond, baseInterval: 100 * time.Millisecond, configuredInterval: 100 * time.Millisecond}
	c.startRateTune()
	now := time.Now()
	finishTuneStage(c, now)
	c.advanceRateTune(now)
	c.throttle("120")
	st := c.rateStatus()
	if st.Tune.State != "bounded" || st.Tune.BoundaryMS == nil || *st.Tune.BoundaryMS != 80 || st.BaseIntervalMS != 100 || st.Tune.LastGoodMS != 100 || len(st.Tune.Trials) != 2 {
		t.Fatal(st)
	}
	for i := 0; i < 5; i++ {
		c.throttle("")
	}
	if c.rateStatus().BaseIntervalMS != 100 {
		t.Fatal("in-flight 429 moved more than one step")
	}
	restored := &Client{Interval: 500 * time.Millisecond}
	restored.restoreRate(c.rateStatus())
	if restored.rateStatus().Tune.State != "bounded" || restored.rateStatus().LimitedUntil != st.LimitedUntil {
		t.Fatal(restored.rateStatus())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if restored.pace(ctx) == nil {
		t.Fatal("rollback bypassed Retry-After")
	}
	restored.mu.Lock()
	restored.cooldown = now.Add(-time.Second)
	restored.backoffLevel = 0
	restored.mu.Unlock()
	restored.throttle("")
	if restored.rateStatus().BaseIntervalMS != 150 {
		t.Fatal("recurring restriction did not slow further", restored.rateStatus())
	}
}
func TestTuneZeroHasRecoveryAndNoAutomaticReprobe(t *testing.T) {
	c := &Client{Interval: 10 * time.Millisecond, baseInterval: 10 * time.Millisecond, configuredInterval: 10 * time.Millisecond}
	c.startRateTune()
	now := time.Now()
	finishTuneStage(c, now)
	c.advanceRateTune(now)
	st := c.rateStatus()
	if st.IntervalMS != 0 || st.ConfiguredIntervalMS != 0 {
		t.Fatal(st)
	}
	restored := &Client{Interval: 500 * time.Millisecond}
	restored.restoreRate(st)
	if restored.Interval != 0 || restored.tune.State != "probing" {
		t.Fatal(restored.rateStatus())
	}
	finishTuneStage(restored, now)
	restored.advanceRateTune(now)
	if restored.rateStatus().Tune.State != "zero_reached" {
		t.Fatal(restored.rateStatus())
	}
	restored.throttle("")
	if restored.rateStatus().BaseIntervalMS != 10 || restored.rateStatus().Tune.State != "bounded" {
		t.Fatal(restored.rateStatus())
	}
	restored.mu.Lock()
	restored.cooldown = time.Time{}
	restored.mu.Unlock()
	for i := 0; i < 100; i++ {
		restored.acceptedRequest()
	}
	if restored.rateStatus().IntervalMS != 10 {
		t.Fatal(restored.rateStatus())
	}
	finishTuneStage(restored, now)
	restored.advanceRateTune(now)
	if restored.rateStatus().IntervalMS != 10 {
		t.Fatal("automatically repeated failed stage")
	}
}
func TestTuneDoesNotProveStageDuringRecovery(t *testing.T) {
	c := &Client{Interval: 100 * time.Millisecond, baseInterval: 100 * time.Millisecond, configuredInterval: 100 * time.Millisecond, cooldown: time.Now().Add(time.Hour), backoffLevel: 1}
	c.startRateTune()
	now := time.Now()
	finishTuneStage(c, now)
	c.advanceRateTune(now)
	if len(c.rateStatus().Tune.Trials) != 0 || c.rateStatus().IntervalMS != 100 {
		t.Fatal(c.rateStatus())
	}
}
