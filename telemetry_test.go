package main

import (
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestRatesCountLogicalCopiesAndSeparateNetworkFromMedia(t *testing.T) {
	now := time.Now()
	samples := []transferSample{{at: now.Add(-time.Minute), done: 100, versions: 150, saved: 1e6, network: 2e6, requests: 10}, {at: now.Add(-10 * time.Second), done: 140, versions: 230, saved: 9e6, network: 12e6, requests: 110}, {at: now, done: 160, versions: 270, saved: 13e6, network: 22e6, requests: 210}}
	st := Stats{Total: 760, Done: 160, Bytes: 13e6, Archives: 270, Inventory: "incomplete"}
	r := transferRates(samples, st, true, false)
	if r.FilesSecond != 1 || r.FilesMinute != 60 || r.FilesHour != 3600 || r.VersionsSecond != 2 || r.NetworkMBPS != 1 || r.SavedMBPS != 0.2 || r.RPS != 10 {
		t.Fatal(r)
	}
	if r.ETASeconds == nil || *r.ETASeconds != 600 || r.ETAScope != "known_files" || r.EstimatedRemainingMediaBytes != 120e6 {
		t.Fatal(r)
	}
	for _, limited := range []bool{false, true} {
		r = transferRates(samples, st, limited, limited)
		if r.FilesSecond != 0 || r.NetworkMBPS != 0 || r.ETASeconds != nil {
			t.Fatal("false speed or ETA while idle/limited", r)
		}
	}
}
func TestRatesDoNotInventETAFromMetadataOrUpgrades(t *testing.T) {
	now := time.Now()
	samples := []transferSample{{at: now.Add(-time.Minute), done: 10}, {at: now, done: 10, versions: 100, saved: 100e6}}
	r := transferRates(samples, Stats{Total: 100, Done: 10}, true, false)
	if r.FilesSecond != 0 || r.ETASeconds != nil || r.ETAState != "no_progress" {
		t.Fatal(r)
	}
	r = transferRates(samples, Stats{Total: 10, Done: 10, Inventory: "incomplete"}, true, false)
	if r.ETAState != "known_complete" || r.ETAScope != "known_files" {
		t.Fatal("incomplete account falsely complete", r)
	}
}
func TestTelemetryResetsClientCountersWithoutNegativeRates(t *testing.T) {
	tm := &Telemetry{}
	first, second := &Client{}, &Client{}
	now := time.Now()
	first.mediaBytes.Store(100e6)
	tm.sample(now, Stats{Done: 10, Bytes: 100e6}, first, true)
	second.mediaBytes.Store(1e6)
	tm.sample(now.Add(time.Minute), Stats{Done: 20, Bytes: 200e6}, second, true)
	r, _ := tm.status()
	if r.NetworkMBPS != 0 || r.FilesSecond != 0 || math.IsNaN(r.RPS) {
		t.Fatal(r)
	}
}
func TestMediaCounterAndStorageMeasureActualFileLengths(t *testing.T) {
	var total atomic.Int64
	io.Copy(io.Discard, &mediaCounter{Reader: bytes.NewReader(make([]byte, 1234)), total: &total})
	if total.Load() != 1234 {
		t.Fatal(total.Load())
	}
	root := t.TempDir()
	os.Mkdir(filepath.Join(root, "archives"), 0700)
	os.WriteFile(filepath.Join(root, "archives", "media.zip"), make([]byte, 2345), 0600)
	os.WriteFile(filepath.Join(root, "state.db"), make([]byte, 3456), 0600)
	s := scanStorage(context.Background(), root)
	if s.Partial || s.FolderBytes != 5801 || s.VolumeTotalBytes == 0 || s.Updated == "" {
		t.Fatal(s)
	}
}
