package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func newFallbackZIPBatch(resources []Resource, pass int64) zipBatchRecord {
	b := newZIPBatch(resources)
	h := sha256.Sum256([]byte(b.ID + ":" + strconv.FormatInt(pass, 10)))
	b.ID = hex.EncodeToString(h[:])
	b.Archive = filepath.ToSlash(filepath.Join("zip-batches", b.ID+".zip"))
	return b
}

func recoverOriginalZIP(ctx context.Context, s *Store, log func(string)) error {
	previous, e := s.pendingZIPBatches()
	if e != nil {
		return &LocalError{e}
	}
	for _, b := range previous {
		full, e := localArchive(s.Root, b.Archive)
		if e != nil {
			return &LocalError{e}
		}
		if _, e := os.Stat(full); os.IsNotExist(e) {
			continue
		} else if e != nil {
			return &LocalError{e}
		}
		var resources []Resource
		for _, id := range b.IDs {
			r, e := s.load(id)
			if e != nil {
				return &LocalError{e}
			}
			resources = append(resources, r)
		}
		matched, missing, e := importZIPBatch(ctx, s, b, resources)
		if e != nil {
			return e
		}
		if e := s.markZIPAttempt(b.IDs, time.Now().UnixNano()); e != nil {
			return e
		}
		log(fmt.Sprintf("Восстановлен скачанный ZIP: %d оригиналов, %d пропусков; повторное скачивание не требуется.", matched, missing))
	}
	return nil
}

func (s *Store) queueOriginalZIP(id string) error {
	_, e := s.DB.Exec("INSERT OR IGNORE INTO original_zip_queue(file_id) SELECT id FROM files WHERE id=? AND "+confirmedSQL("files"), id)
	if e != nil {
		return &LocalError{e}
	}
	return nil
}
func (s *Store) removeOriginalZIP(id string) error {
	_, e := s.DB.Exec("DELETE FROM original_zip_queue WHERE file_id=?", id)
	if e != nil {
		return &LocalError{e}
	}
	return nil
}
func (s *Store) queuedOriginals(start int64, video bool) ([]Resource, error) {
	predicate := "NOT " + downloadVideoSQL
	if video {
		predicate = downloadVideoSQL
	}
	rows, e := s.DB.Query(`SELECT f.id,f.name,COALESCE(NULLIF(json_extract(raw,'$.size'),0),json_extract(raw,'$.meta.size'),0) FROM files f JOIN original_zip_queue q ON q.file_id=f.id WHERE `+confirmedSQL("f")+` AND q.attempted<? AND `+predicate+` AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='original') ORDER BY f.id`, start)
	if e != nil {
		return nil, &LocalError{e}
	}
	defer rows.Close()
	var out []Resource
	for rows.Next() {
		var r Resource
		if e := rows.Scan(&r.ID, &r.Name, &r.Size); e != nil {
			return nil, &LocalError{e}
		}
		out = append(out, r)
	}
	if e := rows.Err(); e != nil {
		return nil, &LocalError{e}
	}
	return out, nil
}
func (s *Store) markZIPAttempt(ids []string, when int64) error {
	tx, e := s.DB.Begin()
	if e != nil {
		return &LocalError{e}
	}
	defer tx.Rollback()
	for _, id := range ids {
		if _, e := tx.Exec("UPDATE original_zip_queue SET attempted=? WHERE file_id=?", when, id); e != nil {
			return &LocalError{e}
		}
		if _, e := tx.Exec("DELETE FROM original_zip_queue WHERE file_id=? AND EXISTS(SELECT 1 FROM versions WHERE file_id=? AND quality='original')", id, id); e != nil {
			return &LocalError{e}
		}
	}
	if e := tx.Commit(); e != nil {
		return &LocalError{e}
	}
	return nil
}

// One sequential ZIP consumer runs alongside direct downloads. TryLock keeps
// the other workers working instead of waiting for a large archive to finish.
func (c *Client) drainOriginalZIP(ctx context.Context, s *Store, flush, video bool, log func(string)) error {
	if flush {
		c.zipFallbackMu.Lock()
	} else if !c.zipFallbackMu.TryLock() {
		return nil
	}
	defer c.zipFallbackMu.Unlock()
	if c.zipFallbackStopped {
		return nil
	}
	if s.zipDisabledUntil().After(time.Now()) {
		c.zipFallbackStopped = true
		status := s.zipStatus()
		status.Stage, status.Error = "unavailable", errZIPDisabled.Error()
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		return errZIPDisabled
	}
	if c.zipFallbackStart == 0 {
		c.zipFallbackStart = time.Now().UnixNano()
	}
	for {
		if !flush {
			predicate := "NOT " + downloadVideoSQL
			if video {
				predicate = downloadVideoSQL
			}
			var count int
			e := s.DB.QueryRow(`SELECT COUNT(*) FROM original_zip_queue q JOIN files f ON f.id=q.file_id WHERE `+confirmedSQL("f")+` AND q.attempted<? AND `+predicate+` AND NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='original')`, c.zipFallbackStart).Scan(&count)
			if e != nil {
				return &LocalError{e}
			}
			if count < 500 {
				return nil
			}
		}
		queued, e := s.queuedOriginals(c.zipFallbackStart, video)
		if e != nil {
			return e
		}
		if len(queued) < 500 && !flush || len(queued) < 2 {
			return nil
		}
		batch := splitZIPBatches(queued, 500)[0]
		// Same basenames and the 8 GiB ceiling may split a large queue.
		// Never use this fallback to request a ZIP containing one resource.
		if len(batch) < 2 {
			return nil
		}
		for i, r := range batch {
			loaded, e := s.load(r.ID)
			if e != nil {
				return &LocalError{e}
			}
			batch[i] = loaded
		}
		status := s.zipStatus()
		if status.Stage == "deferred" || status.BatchSize != 500 {
			status = ZIPStatus{BatchSize: 500}
		}
		status.Batch++
		status.Batches = status.Batch + (len(queued)-len(batch)+499)/500
		status.Stage = "downloading"
		status.Error = ""
		status.Requested += len(batch)
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		log(fmt.Sprintf("ZIP-добор: накоплено %d, запрашиваю пачку из %d оригиналов.", len(queued), len(batch)))
		b := newFallbackZIPBatch(batch, c.zipFallbackStart)
		started := time.Now()
		size, e := c.fetchZIPBatch(ctx, s, b)
		if e != nil {
			c.zipFallbackStopped = true
			status.Stage, status.Error = "deferred", e.Error()
			if errors.Is(e, errZIPDisabled) {
				status.Stage = "unavailable"
			}
			status.Seconds += time.Since(started).Seconds()
			if se := s.saveZIPStatus(status); se != nil {
				return se
			}
			log("Пакетный ZIP отложен; очередь сохранена для следующей проверки. " + e.Error())
			return e
		}
		status.Stage = "verifying"
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		matched, missing, e := importZIPBatch(ctx, s, b, batch)
		if e != nil {
			return e
		}
		if e := s.markZIPAttempt(b.IDs, time.Now().UnixNano()); e != nil {
			return e
		}
		status.Imported += matched
		status.Missing += missing
		status.Archives++
		status.Bytes += size
		status.Stage = "completed"
		status.Seconds += time.Since(started).Seconds()
		if e := s.saveZIPStatus(status); e != nil {
			return e
		}
		log(fmt.Sprintf("ZIP-добор: сохранено %d/%d оригиналов; %d пропусков оставлены фоновой проверке.", matched, len(batch), missing))
		if !flush {
			return nil
		}
	}
}
