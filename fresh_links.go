package main

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// A Photoslice /disk URL may carry its expiry as an eight-digit hexadecimal
// Unix timestamp. Unknown URL layouts are refreshed by age, never guessed.
func originalLinkExpiry(uri string) (time.Time, bool) {
	u, e := url.Parse(uri)
	if e != nil {
		return time.Time{}, false
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) != 4 || parts[0] != "disk" || len(parts[1]) != 64 || len(parts[2]) != 8 {
		return time.Time{}, false
	}
	if _, e := strconv.ParseUint(parts[1][:16], 16, 64); e != nil {
		return time.Time{}, false
	}
	n, e := strconv.ParseInt(parts[2], 16, 64)
	if e != nil || n < 1420070400 || n > 2051222400 {
		return time.Time{}, false
	}
	return time.Unix(n, 0), true
}

func expiredOriginalLink(uri string, now time.Time) bool {
	expiry, ok := originalLinkExpiry(uri)
	return ok && !expiry.After(now)
}

type refreshedCluster struct {
	done  chan struct{}
	until time.Time
	err   error
}

type linkRefresher struct {
	mu           sync.Mutex
	clusters     map[string]*refreshedCluster
	snapshotMu   sync.Mutex
	snapshot     string
	snapshotTime time.Time
}

func newLinkRefresher() *linkRefresher {
	return &linkRefresher{clusters: map[string]*refreshedCluster{}}
}

func (f *linkRefresher) snapshotID(ctx context.Context, c *Client) (string, error) {
	f.snapshotMu.Lock()
	defer f.snapshotMu.Unlock()
	if f.snapshot != "" && time.Since(f.snapshotTime) < 30*time.Minute {
		return f.snapshot, nil
	}
	d, e := c.call(ctx, "intapi/photo-init-snapshot", nil)
	if e != nil {
		return "", e
	}
	id := first(d, "photoslice_id", "photosliceId", "id")
	if id == "" {
		return "", errors.New("обновление ссылок: нет photoslice_id")
	}
	f.snapshot, f.snapshotTime = id, time.Now()
	return id, nil
}

func (f *linkRefresher) cluster(ctx context.Context, c *Client, s *Store, r Resource) error {
	for {
		f.mu.Lock()
		entry := f.clusters[r.Cluster]
		if entry != nil && entry.done != nil {
			done := entry.done
			f.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
			}
			continue
		}
		if entry != nil && time.Now().Before(entry.until) {
			e := entry.err
			f.mu.Unlock()
			return e
		}
		entry = &refreshedCluster{done: make(chan struct{})}
		f.clusters[r.Cluster] = entry
		f.mu.Unlock()

		psid, e := f.snapshotID(ctx, c)
		if e == nil {
			expected, _ := strconv.ParseInt(s.get("cluster_size:"+r.Cluster), 10, 64)
			e = c.collectCluster(ctx, s, psid, r.Cluster, expected)
		}
		until := time.Now().Add(10 * time.Minute)
		if e != nil {
			until = time.Now().Add(30 * time.Second)
		} else {
			// Reload from SQLite, since another queued batch may contain the old
			// URL. Refresh again before a renewed original link can expire.
			if fresh, le := s.load(r.ID); le == nil {
				for _, candidate := range directOriginalCandidates(fresh) {
					if expiry, ok := originalLinkExpiry(candidate.URL); ok && expiry.Add(-2*time.Minute).Before(until) {
						until = expiry.Add(-2 * time.Minute)
					}
				}
			}
			if until.Before(time.Now()) {
				until = time.Now().Add(time.Minute)
			}
		}
		f.mu.Lock()
		entry.until, entry.err = until, e
		close(entry.done)
		entry.done = nil
		f.mu.Unlock()
		return e
	}
}

func (f *linkRefresher) refresh(ctx context.Context, c *Client, s *Store, resources []Resource) ([]Resource, error) {
	out := make([]Resource, 0, len(resources))
	seen := map[string]error{}
	for _, r := range resources {
		var refreshErr error
		if r.Cluster != "" {
			var ok bool
			refreshErr, ok = seen[r.Cluster]
			if !ok {
				refreshErr = f.cluster(ctx, c, s, r)
				seen[r.Cluster] = refreshErr
			}
			if fatal(refreshErr) {
				return nil, refreshErr
			}
			fresh, e := s.load(r.ID)
			if e != nil {
				return nil, &LocalError{e}
			}
			r = fresh
		}
		if refreshErr != nil {
			// Lower copies can still be rescued. A failed refresh is not proof
			// of absence and does not authorize using a stale original URL.
			r.OriginalError = errors.New("обновление ссылки оригинала не удалось; требуется пакетный ZIP")
			r.Candidates = append([]Candidate(nil), r.Candidates...)
			var candidates []Candidate
			for _, candidate := range r.Candidates {
				if candidate.Quality != "original" {
					candidates = append(candidates, candidate)
				}
			}
			r.Candidates = candidates
		}
		out = append(out, r)
	}
	return out, nil
}
