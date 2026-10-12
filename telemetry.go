package main

import (
	"context"
	"io"
	"io/fs"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type mediaCounter struct {
	io.Reader
	total *atomic.Int64
}

func (r *mediaCounter) Read(b []byte) (int, error) {
	n, e := r.Reader.Read(b)
	r.total.Add(int64(n))
	return n, e
}

type StorageStatus struct {
	FolderBytes      int64  `json:"folder_bytes"`
	VolumeTotalBytes uint64 `json:"volume_total_bytes"`
	VolumeFreeBytes  uint64 `json:"volume_free_bytes"`
	VolumeUsedBytes  uint64 `json:"volume_used_bytes"`
	Updated          string `json:"updated,omitempty"`
	Partial          bool   `json:"partial"`
}
type TransferStatus struct {
	NetworkMBPS                  float64 `json:"network_mb_per_second"`
	SavedMBPS                    float64 `json:"saved_mb_per_second"`
	FilesSecond                  float64 `json:"files_per_second"`
	FilesMinute                  float64 `json:"files_per_minute"`
	FilesHour                    float64 `json:"files_per_hour"`
	VersionsSecond               float64 `json:"versions_per_second"`
	RPS                          float64 `json:"rps"`
	WindowSeconds                float64 `json:"window_seconds"`
	ETASampleSeconds             float64 `json:"eta_sample_seconds"`
	Remaining                    int     `json:"remaining_files"`
	ETASeconds                   *int64  `json:"eta_seconds,omitempty"`
	FinishAt                     string  `json:"finish_at,omitempty"`
	ETAState                     string  `json:"eta_state"`
	ETAScope                     string  `json:"eta_scope"`
	EstimatedRemainingMediaBytes int64   `json:"estimated_remaining_media_bytes"`
	MediaBytes                   int64   `json:"media_bytes"`
	LogicalFiles                 int     `json:"logical_files"`
	Versions                     int     `json:"versions"`
}
type transferSample struct {
	at                       time.Time
	done, versions           int
	saved, network, requests int64
}
type Telemetry struct {
	mu       sync.Mutex
	samples  []transferSample
	client   *Client
	transfer TransferStatus
	storage  StorageStatus
}

func sampleAt(samples []transferSample, now time.Time, window time.Duration) transferSample {
	base := samples[0]
	for _, s := range samples {
		if s.at.After(now.Add(-window)) {
			break
		}
		base = s
	}
	return base
}

func transferRates(samples []transferSample, st Stats, running, limited bool) TransferStatus {
	r := TransferStatus{MediaBytes: st.Bytes, LogicalFiles: st.Done, Versions: st.Archives, Remaining: st.ConfirmedNoCopy, ETAState: "measuring", ETAScope: "confirmed_files"}
	if r.Remaining < 0 {
		r.Remaining = 0
	}
	if st.Inventory == "complete" && st.Unconfirmed == 0 {
		r.ETAScope = "full_inventory"
	}
	if len(samples) < 2 {
		return r
	}
	last := samples[len(samples)-1]
	base := sampleAt(samples, last.at, 60*time.Second)
	secs := last.at.Sub(base.at).Seconds()
	r.WindowSeconds = secs
	if secs > 0 && running && !limited {
		r.FilesSecond = math.Max(0, float64(last.done-base.done)/secs)
		r.FilesMinute = r.FilesSecond * 60
		r.FilesHour = r.FilesSecond * 3600
		r.VersionsSecond = math.Max(0, float64(last.versions-base.versions)/secs)
		r.SavedMBPS = math.Max(0, float64(last.saved-base.saved)/secs/1e6)
		short := sampleAt(samples, last.at, 10*time.Second)
		if d := last.at.Sub(short.at).Seconds(); d > 0 {
			r.NetworkMBPS = math.Max(0, float64(last.network-short.network)/d/1e6)
			r.RPS = math.Max(0, float64(last.requests-short.requests)/d)
		}
	}
	switch {
	case r.Remaining == 0:
		r.ETAState = "known_complete"
		zero := int64(0)
		r.ETASeconds = &zero
	case limited:
		r.ETAState = "rate_limited"
	case !running:
		r.ETAState = "idle"
	default:
		b := sampleAt(samples, last.at, 5*time.Minute)
		d := last.at.Sub(b.at).Seconds()
		r.ETASampleSeconds = d
		count := last.done - b.done
		if d >= 30 && count >= 5 && r.FilesSecond > 0 {
			// Logical files count once, regardless of how many quality versions
			// were obtained. Failed attempts/metadata never increase this rate.
			eta := int64(math.Ceil(float64(r.Remaining) * d / float64(count)))
			r.ETASeconds = &eta
			r.ETAState = "estimate"
			r.FinishAt = last.at.Add(time.Duration(eta) * time.Second).UTC().Format(time.RFC3339)
			if bytes := last.saved - b.saved; bytes > 0 {
				r.EstimatedRemainingMediaBytes = int64(float64(bytes) * float64(r.Remaining) / float64(count))
			}
		} else if d >= 60 && count == 0 {
			r.ETAState = "no_progress"
		}
	}
	return r
}

func (t *Telemetry) sample(now time.Time, st Stats, client *Client, running bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.client != client {
		t.samples = nil
		t.client = client
	}
	p := transferSample{at: now, done: st.Done, versions: st.Archives, saved: st.Bytes}
	limited := false
	if client != nil {
		p.network = client.mediaBytes.Load()
		r := client.rateStatus()
		p.requests = r.Requests
		limited = r.LimitedUntil != ""
	}
	t.samples = append(t.samples, p)
	for len(t.samples) > 1 && t.samples[1].at.Before(now.Add(-5*time.Minute)) {
		t.samples = t.samples[1:]
	}
	t.transfer = transferRates(t.samples, st, running, limited)
}
func (t *Telemetry) status() (TransferStatus, StorageStatus) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.transfer, t.storage
}

func scanStorage(ctx context.Context, root string) StorageStatus {
	r := StorageStatus{}
	e := filepath.WalkDir(root, func(_ string, d fs.DirEntry, e error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e != nil {
			r.Partial = true
			return nil
		}
		if d.IsDir() || d.Type()&fs.ModeSymlink != 0 {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			r.Partial = true
			return nil
		}
		if info.Mode().IsRegular() {
			r.FolderBytes += info.Size()
		}
		return nil
	})
	if e != nil {
		r.Partial = true
	}
	r.VolumeTotalBytes, r.VolumeFreeBytes, e = volumeSpace(root)
	if e != nil {
		r.Partial = true
	} else if r.VolumeTotalBytes >= r.VolumeFreeBytes {
		r.VolumeUsedBytes = r.VolumeTotalBytes - r.VolumeFreeBytes
	}
	r.Updated = time.Now().UTC().Format(time.RFC3339)
	return r
}

func (t *Telemetry) run(ctx context.Context, s *Store, j *Job) {
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		st, e := s.stats()
		if e == nil {
			j.mu.Lock()
			c, active := j.client, j.running
			j.mu.Unlock()
			t.sample(time.Now(), st, c, active)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (t *Telemetry) runStorage(ctx context.Context, root string) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		s := scanStorage(ctx, root)
		t.mu.Lock()
		t.storage = s
		t.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
