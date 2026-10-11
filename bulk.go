package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

func (c *Client) processBatch(ctx context.Context, s *Store, resources []Resource, log func(string)) error {
	if e := c.processBatchStage(ctx, s, resources, "previews", log); e != nil {
		return e
	}
	return c.processBatchStage(ctx, s, resources, "originals", log)
}

func (c *Client) processBatchStage(ctx context.Context, s *Store, resources []Resource, stage string, log func(string)) error {
	if c.FreshLinks != nil {
		var e error
		resources, e = c.FreshLinks.refresh(ctx, c, s, resources)
		if e != nil {
			return e
		}
	}
	if stage == "originals" {
		_, cleanup, e := c.prefetch(ctx, s, resources)
		defer cleanup()
		if e != nil {
			if fatal(e) {
				return e
			}
			log("Пакетный оригинал не получен; проверяю файлы отдельно.")
		}
	}
	qualities := []string{"original"}
	if stage == "previews" {
		qualities = []string{"preview", "thumbnail"}
	}
	for _, r := range resources {
		if e := ctx.Err(); e != nil {
			return e
		}
		present, e := s.versions(r.ID)
		if e != nil {
			return e
		}
		r.FailedURLs = make(map[string]error)
		var last error
		for _, quality := range qualities {
			// A thumbnail is a fallback copy, never extra work after a preview
			// or an already saved original. Existing thumbnails are retained.
			_, hasPreview := present["preview"]
			_, hasOriginal := present["original"]
			if quality == "thumbnail" && (hasPreview || hasOriginal) {
				continue
			}
			if _, ok := present[quality]; ok {
				continue
			}
			if e := ctx.Err(); e != nil {
				return e
			}
			result, e := c.downloadSelected(ctx, s, r, quality)
			if e != nil {
				if fatal(e) {
					return e
				}
				if e := s.attempt(r.ID, quality, e); e != nil {
					return e
				}
				if quality == "original" && c.BatchOriginals {
					if e := s.queueOriginalZIP(r.ID); e != nil {
						return e
					}
				}
				last = e
				continue
			}
			if e := s.done(result); e != nil {
				return e
			}
			present[result.Quality] = result
			if result.Quality == "original" && c.BatchOriginals {
				if e := s.removeOriginalZIP(r.ID); e != nil {
					return e
				}
			}
			log(fmt.Sprintf("Сохранён: %s · %s · %.1f МБ", safeName(r.Name), result.Quality, float64(result.Bytes)/1e6))
		}
		// During the preview pass an original has not been attempted yet.
		// Keep a copyless file pending until that second pass determines its result.
		if stage == "originals" && len(present) == 0 && last != nil {
			if e := s.failed(r.ID, last); e != nil {
				return e
			}
			log("Без копии: " + safeName(r.Name))
		}
	}
	return nil
}

func (c *Client) prefetch(ctx context.Context, s *Store, resources []Resource) (string, func(), error) {
	cleanup := func() {}
	if c.BatchOriginals {
		return "", cleanup, nil
	}
	for _, r := range resources {
		if len(directOriginalCandidates(r)) > 0 {
			// A missing-link item in a mixed batch must not block ready URLs
			// while waiting for server-side ZIP preparation.
			return "", cleanup, nil
		}
	}
	counts := map[string]int{}
	for _, r := range resources {
		counts[r.Name]++
	}
	var paths []string
	var eligible []int
	for i, r := range resources {
		present, e := s.versions(r.ID)
		if e != nil {
			return "", cleanup, e
		}
		if _, ok := present["original"]; ok || counts[r.Name] != 1 || len(directOriginalCandidates(r)) > 0 {
			continue
		}
		paths = append(paths, r.ID)
		eligible = append(eligible, i)
	}
	if len(paths) < 2 {
		return "", cleanup, nil
	}
	dir, e := os.MkdirTemp(filepath.Join(s.Root, "tmp"), "bulk-")
	if e != nil {
		return "", cleanup, &LocalError{e}
	}
	cleanup = func() { os.RemoveAll(dir) }
	d, e := c.call(ctx, "mpfs/bulk-download-prepare", Object{"items": paths})
	if e != nil {
		if isProbeTimeout(e) {
			for _, i := range eligible {
				resources[i].OriginalError = e
			}
		}
		return "", cleanup, e
	}
	uri := first(d, "download_url")
	if uri == "" {
		return "", cleanup, errors.New("bulk: нет download_url")
	}
	file := filepath.Join(dir, "originals.zip.part")
	if e := c.media(ctx, uri, file); e != nil {
		if isProbeTimeout(e) {
			for _, i := range eligible {
				resources[i].OriginalError = e
			}
		}
		return "", cleanup, e
	}
	for _, i := range eligible {
		resources[i].OriginalZIP = file
	}
	return file, cleanup, nil
}
