package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func verifySaved(ctx context.Context, s *Store, log func(string)) error {
	return verifyVersions(ctx, s, true, log)
}
func verifyVersions(ctx context.Context, s *Store, full bool, log func(string)) error {
	ids, e := s.ids("EXISTS(SELECT 1 FROM versions WHERE file_id=files.id)")
	if e != nil {
		return e
	}
	for i, id := range ids {
		if e := ctx.Err(); e != nil {
			return e
		}
		versions, e := s.versions(id)
		if e != nil {
			return e
		}
		for quality, r := range versions {
			file, e := localArchive(s.Root, r.Archive)
			var st os.FileInfo
			if e == nil {
				st, e = os.Stat(file)
			}
			unchanged := e == nil && r.ArchiveSize > 0 && st.Size() == r.ArchiveSize && st.ModTime().UnixNano() == r.ArchiveMtime
			if e == nil && (full || !unchanged) {
				e = verifyZIP(ctx, file, r)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if e != nil {
				if e := s.removeVersion(id, quality, "архив версии отсутствует или повреждён"); e != nil {
					return e
				}
				log("Повторить версию " + quality + ": " + id)
			} else if !unchanged {
				r.ArchiveSize = st.Size()
				r.ArchiveMtime = st.ModTime().UnixNano()
				if e := s.done(r); e != nil {
					return e
				}
			}
		}
		if i%1000 == 0 {
			log(fmt.Sprintf("Проверка сохранённых файлов: %d / %d", i+1, len(ids)))
		}
	}
	return nil
}
func runBackup(ctx context.Context, s *Store, cookies string, workers int, log func(string)) error {
	return runMode(ctx, s, cookies, workers, false, log)
}
func runMode(ctx context.Context, s *Store, cookies string, workers int, upgrade bool, log func(string)) error {
	mode := "run"
	if upgrade {
		mode = "upgrade"
	}
	return runSelected(ctx, s, cookies, workers, mode, log, nil)
}
func runSelected(ctx context.Context, s *Store, cookies string, workers int, mode string, log func(string), attach func(*Client)) error {
	c, e := newClient(cookies)
	if e != nil {
		return e
	}
	defer c.HTTP.CloseIdleConnections()
	if e := c.loadWebDAVToken(filepath.Join(s.Root, "oauth-token.txt")); e != nil {
		return e
	}
	if c.WebDAVToken != "" {
		log("Оригиналы: прямой WebDAV GET с OAuth, как в Android; затем доступные веб-ссылки.")
	}
	c.BatchOriginals = true
	c.FreshLinks = newLinkRefresher()
	c.zipFallbackStart = time.Now().UnixNano()
	var savedRate RateStatus
	json.Unmarshal([]byte(s.get("rate_state")), &savedRate)
	c.restoreRate(savedRate)
	c.Log = log
	if mode == "zip-test" {
		c.ZIPBatchSize = 20
		c.ZIPMaxBatches = 1
	}
	c.AssetSink = func(assets []string) {
		if len(assets) == 0 {
			return
		}
		if e := atomicFile(filepath.Join(s.Root, "client-assets.json"), func(f *os.File) error { return json.NewEncoder(f).Encode(assets) }); e != nil {
			log("Не удалось сохранить список публичных JS клиента для диагностики.")
		}
	}
	c.Checkpoint = func(st RateStatus) error {
		b, e := json.Marshal(st)
		if e != nil {
			return e
		}
		return s.set("rate_state", string(b))
	}
	if e := c.saveRate(); e != nil {
		return e
	}
	if !strings.HasPrefix(mode, "zip") {
		if e := s.saveZIPStatus(ZIPStatus{Stage: "accumulating", BatchSize: 500}); e != nil {
			return e
		}
	}
	if attach != nil {
		attach(c)
	}
	go c.runRateTune(ctx)
	return runInventory(ctx, s, c, workers, mode, log)
}
func runWithClient(ctx context.Context, s *Store, c *Client, workers int, log func(string)) error {
	return runOptions(ctx, s, c, workers, false, log)
}
func runOptions(ctx context.Context, s *Store, c *Client, workers int, upgrade bool, log func(string)) error {
	mode := "run"
	if upgrade {
		mode = "upgrade"
	}
	return runInventory(ctx, s, c, workers, mode, log)
}
func runInventory(ctx context.Context, s *Store, c *Client, workers int, mode string, log func(string)) error {
	unlock, e := lockRoot(s.Root)
	if e != nil {
		return e
	}
	defer unlock()
	n, e := s.requeueRateLimits()
	if e != nil {
		return e
	}
	if n > 0 {
		log(fmt.Sprintf("Возвращены в очередь после временного HTTP 429: %d файлов", n))
	}
	if e := os.MkdirAll(filepath.Join(s.Root, "tmp"), 0700); e != nil {
		return e
	}
	log("Проверяю авторизованную сессию Яндекса…")
	if e := c.auth(ctx); e != nil {
		return e
	}
	c.mu.Lock()
	uid := c.uid
	c.mu.Unlock()
	if e := s.checkAccount(uid); e != nil {
		return e
	}
	if e := verifyVersions(ctx, s, false, log); e != nil {
		return e
	}
	var collectErr error
	if mode == "resume" || strings.HasPrefix(mode, "zip") {
		log("Продолжаю по сохранённому списку; повторного обхода Photoslice нет.")
		if s.get("inventory") != "complete" {
			log("Полнота ранее собранного списка не подтверждена; этот статус сохраняется.")
		}
	} else if mode == "upgrade" {
		if c.FreshLinks != nil {
			log("Обновляю ссылки небольшими группами непосредственно перед скачиванием.")
		} else {
			log("Обновляю метаданные файлов без оригиналов…")
			collectErr = c.refreshKnown(ctx, s, log)
		}
	} else {
		log("Читаю полный список Photoslice…")
		collectErr = c.collect(ctx, s, log)
	}
	if collectErr != nil {
		if fatal(collectErr) {
			return collectErr
		}
		log("Ошибка обновления списка: " + collectErr.Error())
	}
	// The whole preview pass must finish before any original request, even
	// when a fast worker has run out of previews or a saved ZIP queue is full.
	if mode != "zip-test" {
		if e := runDownloadStage(ctx, s, c, workers, "previews", log); e != nil {
			return e
		}
	}
	c.mu.Lock()
	c.downloadStage = "originals"
	c.mu.Unlock()
	if c.BatchOriginals {
		if e := recoverOriginalZIP(ctx, s, log); e != nil {
			return e
		}
	}
	if strings.HasPrefix(mode, "zip") {
		e := runZIPPass(ctx, s, c, log)
		if mode == "zip-test" || fatal(e) {
			return e
		}
		if e != nil {
			log("Пакетный ZIP сейчас недоступен: " + e.Error() + ". Продолжаю отдельные загрузки; общая пауза Яндекса сохраняется.")
		} else {
			log("Проход ZIP завершён. Пропуски, превью и миниатюры остаются для отдельной периодической проверки. Пачки с пропусками сохранены.")
			return nil
		}
	}
	if e := runDownloadStage(ctx, s, c, workers, "originals", log); e != nil {
		return e
	}
	if e := s.report(); e != nil {
		return e
	}
	st, e := s.stats()
	if e != nil {
		return e
	}
	log(fmt.Sprintf("Файлы: %d / %d; версии: %d; без оригиналов: %d; без копии: %d", st.Done, st.Total, st.Archives, st.NoOriginal, st.Total-st.Done))
	if collectErr != nil {
		return collectErr
	}
	if (mode == "resume" || mode == "zip") && st.Inventory != "complete" {
		return fmt.Errorf("проход по сохранённому списку завершён; полнота списка не подтверждена")
	}
	if st.Failed+st.Pending > 0 {
		return fmt.Errorf("частичный архив: %d файлов без копии; подробности в отчёте", st.Total-st.Done)
	}
	return nil
}

func runDownloadStage(ctx context.Context, s *Store, c *Client, workers int, stage string, log func(string)) error {
	c.mu.Lock()
	c.downloadStage = stage
	c.mu.Unlock()
	quality, label := "original", "Оригиналы"
	if stage == "previews" {
		quality, label = "preview", "Превью"
	}
	where := "NOT EXISTS(SELECT 1 FROM versions WHERE file_id=files.id AND quality='" + quality + "')"
	seen := make(map[string]bool)
	for {
		queue, e := s.downloadQueue(where)
		if e != nil {
			return e
		}
		pending := queue[:0]
		for _, item := range queue {
			if !seen[item.ID] {
				seen[item.ID] = true
				pending = append(pending, item)
			}
		}
		if len(pending) == 0 {
			break
		}
		if e := runDownloadQueue(ctx, s, c, workers, stage, label, pending, log); e != nil {
			return e
		}
		// Cluster refresh may discover files absent from the starting queue.
		// Attempt their previews too; failures are not retried within this pass.
		if stage != "previews" {
			break
		}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	log(label + ": проход по известным файлам завершён.")
	return nil
}

func runDownloadQueue(ctx context.Context, s *Store, c *Client, workers int, stage, label string, queue []downloadItem, log func(string)) error {
	photos, videos := 0, 0
	for _, item := range queue {
		if item.Video {
			videos++
		} else {
			photos++
		}
	}
	log(fmt.Sprintf("%s: сначала фотографии (%d), затем видео (%d); загрузок: %d", label, photos, videos, workers))
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan []Resource, workers)
	var wg sync.WaitGroup
	var batches sync.WaitGroup
	var firstErr error
	var mu sync.Mutex
	fail := func(e error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = e
		}
		mu.Unlock()
		cancel()
	}
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				if workCtx.Err() != nil {
					batches.Done()
					continue
				}
				if e := c.processBatchStage(workCtx, s, batch, stage, log); e != nil {
					if !errors.Is(e, context.Canceled) {
						fail(e)
					}
				}
				if workCtx.Err() == nil && c.BatchOriginals && stage == "originals" {
					if e := c.drainOriginalZIP(workCtx, s, false, false, log); fatal(e) {
						fail(e)
					}
				}
				batches.Done()
			}
		}()
	}
	batchSize := c.Batch
	if batchSize < 1 {
		batchSize = 1
	}
	var batch []Resource
	var batchBytes int64
	var batchDirect bool
	enqueue := func(resources []Resource) bool {
		batches.Add(1)
		select {
		case jobs <- resources:
			return true
		case <-workCtx.Done():
			batches.Done()
			return false
		}
	}
	sendBatch := func() bool {
		if len(batch) == 0 {
			return true
		}
		if enqueue(batch) {
			batch = nil
			batchBytes = 0
			return true
		}
		return false
	}
	videoPhase := false
send:
	for _, item := range queue {
		if item.Video && !videoPhase {
			if !sendBatch() {
				break send
			}
			// No video request starts while any photo batch is still in flight.
			// Workers drain cancelled batches too, so cancellation cannot strand Wait.
			batches.Wait()
			if workCtx.Err() != nil {
				break send
			}
			if c.BatchOriginals && stage == "originals" {
				if e := c.drainOriginalZIP(workCtx, s, true, false, log); fatal(e) {
					fail(e)
					break send
				}
			}
			videoPhase = true
			log(label + " фотографий проверены. Перехожу к видео.")
		}
		r, e := s.load(item.ID)
		if e != nil {
			fail(e)
			break send
		}
		estimate := r.Size
		direct := len(directOriginalCandidates(r)) > 0
		if len(batch) > 0 && direct != batchDirect {
			if !sendBatch() {
				break send
			}
		}
		batchDirect = direct
		unknownVideo := r.Size <= 0 && item.Video
		if unknownVideo {
			if !sendBatch() {
				break send
			}
			if !enqueue([]Resource{r}) {
				break send
			}
			continue
		}
		if estimate <= 0 {
			estimate = 8 << 20
		}
		if len(batch) > 0 && (len(batch) >= batchSize || batchBytes+estimate > 128<<20) {
			if !sendBatch() {
				break send
			}
		}
		batch = append(batch, r)
		batchBytes += estimate
		if len(batch) >= batchSize || batchBytes >= 128<<20 {
			if !sendBatch() {
				break send
			}
		}
	}
	if workCtx.Err() == nil {
		sendBatch()
	}
	close(jobs)
	wg.Wait()
	if workCtx.Err() == nil && c.BatchOriginals && stage == "originals" {
		if e := c.drainOriginalZIP(workCtx, s, true, false, log); fatal(e) {
			return e
		}
		if e := c.drainOriginalZIP(workCtx, s, true, true, log); fatal(e) {
			return e
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if firstErr != nil {
		return firstErr
	}
	return nil
}
