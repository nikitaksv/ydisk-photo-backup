package main

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const inventoryInterval = 30 * time.Minute

type liveTask struct {
	id, stage             string
	video, active, direct bool
	due                   time.Time
	attempts              int
	order                 int64
}

type taskHeap []*liveTask

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].order < h[j].order
	}
	return h[i].due.Before(h[j].due)
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(v any)   { *h = append(*h, v.(*liveTask)) }
func (h *taskHeap) Pop() any     { a := *h; v := a[len(a)-1]; *h = a[:len(a)-1]; return v }

type LiveStatus struct {
	Workers          int    `json:"workers"`
	PreviewWorkers   int    `json:"preview_workers"`
	OriginalWorkers  int    `json:"original_workers"`
	Previews         int    `json:"previews"`
	Originals        int    `json:"originals"`
	Retrying         int    `json:"retrying"`
	NextRetry        string `json:"next_retry,omitempty"`
	NextInventory    string `json:"next_inventory_check,omitempty"`
	InventoryRunning bool   `json:"inventory_running"`
	Verifying        bool   `json:"verifying_archives"`
}

type liveQueue struct {
	mu               sync.Mutex
	reloadMu         sync.Mutex
	loadedCount      int
	loaded           bool
	tasks            map[string]*liveTask
	queues           [2][2]taskHeap
	serial           int64
	workers          int
	active           [2]int
	photoActive      int
	zipBusy          bool
	zipUntil         time.Time
	nextInventory    time.Time
	inventoryRunning bool
	verifying        bool
}

func taskKey(id, stage string) string { return stage + "\x00" + id }
func stageIndex(stage string) int {
	if stage == "originals" {
		return 1
	}
	return 0
}
func mediaIndex(video bool) int {
	if video {
		return 1
	}
	return 0
}

func (q *liveQueue) snapshot() LiveStatus {
	q.mu.Lock()
	defer q.mu.Unlock()
	r := LiveStatus{Workers: q.workers, PreviewWorkers: q.active[0], OriginalWorkers: q.active[1], InventoryRunning: q.inventoryRunning, Verifying: q.verifying}
	if !q.nextInventory.IsZero() {
		r.NextInventory = q.nextInventory.Format(time.RFC3339)
	}
	var next time.Time
	for _, t := range q.tasks {
		if t.stage == "previews" {
			r.Previews++
		} else {
			r.Originals++
		}
		if !t.active && t.due.After(time.Now()) {
			r.Retrying++
			if next.IsZero() || t.due.Before(next) {
				next = t.due
			}
		}
	}
	if !next.IsZero() {
		r.NextRetry = next.Format(time.RFC3339)
	}
	return r
}

// Every task is owned by at most one worker; preferred pools may steal ready work.
func (q *liveQueue) take(preferred int, zip bool, now time.Time) ([]*liveTask, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for media := 0; media < 2; media++ {
		if media == 1 && q.photoActive > 0 {
			continue
		}
		for _, stage := range []int{preferred, 1 - preferred} {
			h := &q.queues[stage][media]
			if h.Len() == 0 || (*h)[0].due.After(now) {
				continue
			}
			useZIP := zip && stage == 1 && media == 0 && !now.Before(q.zipUntil) && !(*h)[0].direct
			if useZIP && q.zipBusy {
				continue
			}
			tasks := []*liveTask{heap.Pop(h).(*liveTask)}
			if useZIP {
				for h.Len() > 0 && len(tasks) < 500 && !(*h)[0].due.After(now) && !(*h)[0].direct {
					tasks = append(tasks, heap.Pop(h).(*liveTask))
				}
				useZIP = len(tasks) > 1
			}
			for _, t := range tasks {
				t.active = true
			}
			q.active[stage]++
			if media == 0 {
				q.photoActive++
			}
			if useZIP {
				q.zipBusy = true
			}
			return tasks, useZIP
		}
	}
	return nil, false
}

func retryDelay(attempt int) time.Duration {
	return min(30*time.Second*time.Duration(1<<min(max(attempt-1, 0), 4)), 5*time.Minute)
}

func (q *liveQueue) reload(s *Store) error {
	q.reloadMu.Lock()
	defer q.reloadMu.Unlock()
	var count int
	if e := s.DB.QueryRow("SELECT COUNT(*) FROM files WHERE confirmed=1").Scan(&count); e != nil {
		return &LocalError{e}
	}
	// Completions/retries are updated by their owner; only new confirmations need a DB queue scan.
	if q.loaded && count == q.loadedCount {
		return nil
	}
	type retry struct {
		attempts int
		due      int64
	}
	retries := map[string]retry{}
	rows, e := s.DB.Query("SELECT file_id,quality,attempts,next_attempt FROM download_retries")
	if e != nil {
		return &LocalError{e}
	}
	for rows.Next() {
		var id, quality string
		var r retry
		if e = rows.Scan(&id, &quality, &r.attempts, &r.due); e != nil {
			rows.Close()
			return &LocalError{e}
		}
		retries[quality+"\x00"+id] = r
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return &LocalError{e}
	}
	for _, stage := range []string{"previews", "originals"} {
		quality := "preview"
		if stage == "originals" {
			quality = "original"
		}
		items, e := s.downloadQueue("NOT EXISTS(SELECT 1 FROM versions WHERE file_id=files.id AND quality='" + quality + "')")
		if e != nil {
			return &LocalError{e}
		}
		for _, item := range items {
			key := taskKey(item.ID, stage)
			q.mu.Lock()
			_, exists := q.tasks[key]
			q.mu.Unlock()
			if exists {
				continue
			}
			r := retries[quality+"\x00"+item.ID]
			t := &liveTask{id: item.ID, stage: stage, video: item.Video, attempts: r.attempts}
			if r.due > 0 {
				t.due = time.Unix(0, r.due)
			}
			q.mu.Lock()
			if _, exists := q.tasks[key]; !exists {
				q.serial++
				t.order = q.serial
				q.tasks[key] = t
				heap.Push(&q.queues[stageIndex(stage)][mediaIndex(item.Video)], t)
			}
			q.mu.Unlock()
		}
	}
	q.loadedCount = count
	q.loaded = true
	return nil
}

func (q *liveQueue) finish(s *Store, tasks []*liveTask, zip bool, zipErr error) error {
	defer func() {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.active[stageIndex(tasks[0].stage)]--
		if !tasks[0].video {
			q.photoActive--
		}
		if zip {
			q.zipBusy = false
			if zipErr != nil {
				q.zipUntil = time.Now().Add(5 * time.Minute)
				if errors.Is(zipErr, errZIPDisabled) {
					q.zipUntil = time.Now().Add(6 * time.Hour)
				}
			}
		}
	}()
	for _, t := range tasks {
		quality := "preview"
		if t.stage == "originals" {
			quality = "original"
		}
		versions, e := s.versions(t.id)
		if e != nil {
			return &LocalError{e}
		}
		_, saved := versions[quality]
		due := time.Time{}
		if !saved {
			if zip {
				due = time.Now()
			} else {
				t.attempts++
				due = time.Now().Add(retryDelay(t.attempts))
			}
			if _, e = s.DB.Exec(`INSERT INTO download_retries(file_id,quality,attempts,next_attempt) VALUES(?,?,?,?) ON CONFLICT(file_id,quality) DO UPDATE SET attempts=excluded.attempts,next_attempt=excluded.next_attempt`, t.id, quality, t.attempts, due.UnixNano()); e != nil {
				return &LocalError{e}
			}
		} else if _, e = s.DB.Exec("DELETE FROM download_retries WHERE file_id=? AND quality=?", t.id, quality); e != nil {
			return &LocalError{e}
		}
		q.mu.Lock()
		if saved {
			delete(q.tasks, taskKey(t.id, t.stage))
		} else {
			t.active = false
			t.due = due
			t.direct = zip
			heap.Push(&q.queues[stageIndex(t.stage)][mediaIndex(t.video)], t)
		}
		q.mu.Unlock()
	}
	return nil
}

func liveZIP(ctx context.Context, s *Store, c *Client, resources []Resource, log func(string)) error {
	// Split duplicate names/size limits instead of guessing renamed ZIP entries.
	for _, batch := range splitZIPBatches(resources, 500) {
		if !c.ZIPEnabled.Load() {
			return errors.New("серверный ZIP выключен; продолжаю напрямую")
		}
		if len(batch) < 2 {
			if e := c.processBatchStage(ctx, s, batch, "originals", log); e != nil {
				return e
			}
			continue
		}
		st := s.zipStatus()
		st.Stage = "downloading"
		st.BatchSize = 500
		st.Batch++
		st.Requested += len(batch)
		st.Error = ""
		if e := s.saveZIPStatus(st); e != nil {
			return e
		}
		b := newFallbackZIPBatch(batch, time.Now().UnixNano())
		started := time.Now()
		size, e := c.fetchZIPBatch(ctx, s, b)
		if e == nil {
			var matched, missing int
			matched, missing, e = importZIPBatch(ctx, s, b, batch)
			if e == nil {
				st.Imported += matched
				st.Missing += missing
				st.Archives++
				st.Bytes += size
				st.Stage = "completed"
			}
		}
		st.Seconds += time.Since(started).Seconds()
		if e != nil {
			st.Stage = "deferred"
			st.Error = e.Error()
			if errors.Is(e, errZIPDisabled) {
				st.Stage = "unavailable"
			}
		}
		if se := s.saveZIPStatus(st); se != nil {
			return se
		}
		if e != nil {
			log("ZIP недоступен; возвращаю файлы в прямую загрузку: " + e.Error())
			return e
		}
	}
	return nil
}

func runLiveWorkers(ctx context.Context, s *Store, c *Client, q *liveQueue, workers int, log func(string)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	fail := func(e error) { once.Do(func() { first = e; cancel() }) }
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(preferred int) {
			defer wg.Done()
			for ctx.Err() == nil {
				tasks, zip := q.take(preferred, c.ZIPEnabled.Load(), time.Now())
				if len(tasks) == 0 {
					if waitContext(ctx, time.Second) != nil {
						return
					}
					continue
				}
				var resources []Resource
				for _, t := range tasks {
					r, e := s.load(t.id)
					if e != nil {
						fail(&LocalError{e})
						return
					}
					resources = append(resources, r)
				}
				var e error
				if zip {
					e = liveZIP(ctx, s, c, resources, log)
				} else {
					e = c.processBatchStage(ctx, s, resources, tasks[0].stage, log)
				}
				if fatal(e) {
					fail(e)
					return
				}
				if fe := q.finish(s, tasks, zip, e); fe != nil {
					fail(fe)
					return
				}
			}
		}(i % 2)
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return ctx.Err()
}

func runContinuous(ctx context.Context, s *Store, c *Client, workers int, log func(string)) error {
	if e := recoverOriginalZIP(ctx, s, log); e != nil {
		return e
	}
	q := &liveQueue{tasks: map[string]*liveTask{}, workers: workers, zipUntil: s.zipDisabledUntil()}
	if e := q.reload(s); e != nil {
		return e
	}
	c.mu.Lock()
	c.downloadStage = "live"
	c.live = q
	c.mu.Unlock()
	log(fmt.Sprintf("Непрерывная загрузка: превью %d, оригиналы %d; свободные воркеры берут другую очередь. Кластеры обновляются каждые 30 минут.", (workers+1)/2, workers/2))
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var bg sync.WaitGroup
	defer bg.Wait()
	defer cancel()
	errCh := make(chan error, 3)
	var verified atomic.Bool
	q.mu.Lock()
	q.verifying = true
	q.mu.Unlock()
	bg.Add(1)
	go func() {
		defer bg.Done()
		e := verifyVersions(ctx, s, false, func(message string) {
			if strings.HasPrefix(message, "Повторить версию ") {
				q.reloadMu.Lock()
				q.loaded = false
				q.reloadMu.Unlock()
			}
			log(message)
		})
		if e != nil {
			if ctx.Err() == nil {
				errCh <- &LocalError{e}
			}
			return
		}
		q.mu.Lock()
		q.verifying = false
		q.mu.Unlock()
		verified.Store(true)
	}()
	bg.Add(1)
	go func() { defer bg.Done(); errCh <- runLiveWorkers(ctx, s, c, q, workers, log) }()
	bg.Add(1)
	go func() {
		defer bg.Done()
		for {
			next, _ := time.Parse(time.RFC3339Nano, s.get("inventory_next"))
			q.mu.Lock()
			q.nextInventory = next
			q.mu.Unlock()
			if e := waitContext(ctx, max(time.Until(next), 0)); e != nil {
				return
			}
			q.mu.Lock()
			q.inventoryRunning = true
			q.mu.Unlock()
			log("Обновляю список кластеров в фоне; скачивание продолжается.")
			started := time.Now()
			e := c.collectCatalog(ctx, s, true, log)
			if fatal(e) {
				errCh <- e
				return
			}
			if e != nil {
				log("Обновление списка: " + e.Error())
			}
			if e = c.reconcileReferences(ctx, s, true, log); fatal(e) {
				errCh <- e
				return
			}
			next = started.Add(inventoryInterval)
			if next.Before(time.Now()) {
				next = time.Now().Add(time.Second)
			}
			if e = s.set("inventory_next", next.UTC().Format(time.RFC3339Nano)); e != nil {
				errCh <- &LocalError{e}
				return
			}
			q.mu.Lock()
			q.inventoryRunning = false
			q.nextInventory = next
			q.mu.Unlock()
			if e = q.reload(s); e != nil {
				errCh <- e
				return
			}
		}
	}()
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	report := time.NewTicker(5 * time.Minute)
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-errCh:
			return e
		case <-report.C:
			if e := s.report(); e != nil {
				return &LocalError{e}
			}
		case <-tick.C:
			if e := q.reload(s); e != nil {
				return e
			}
			st, e := s.stats()
			if e != nil {
				return &LocalError{e}
			}
			if verified.Load() && st.Confirmed > 0 && st.NoOriginal == 0 && st.NoPreview == 0 && st.Unconfirmed == 0 && st.Inventory == "complete" {
				log("Все подтверждённые оригиналы и превью сохранены; полный список подтверждён.")
				return s.report()
			}
		}
	}
}
