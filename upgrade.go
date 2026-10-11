package main

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

func (c *Client) refreshKnown(ctx context.Context, s *Store, log func(string)) error {
	rows, e := s.DB.Query(`SELECT DISTINCT cluster FROM files f WHERE (SELECT COUNT(*) FROM versions WHERE file_id=f.id)<3`)
	if e != nil {
		return e
	}
	var clusters []string
	for rows.Next() {
		var cl string
		if e := rows.Scan(&cl); e != nil {
			rows.Close()
			return e
		}
		clusters = append(clusters, cl)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if len(clusters) == 0 {
		return nil
	}
	d, e := c.call(ctx, "intapi/photo-init-snapshot", nil)
	if e != nil {
		return e
	}
	psid := first(d, "photoslice_id", "photosliceId", "id")
	if psid == "" {
		return fmt.Errorf("нет photoslice_id")
	}
	var firstError error
	for i, cl := range clusters {
		expected, _ := strconv.ParseInt(s.get("cluster_size:"+cl), 10, 64)
		if e := c.collectCluster(ctx, s, psid, cl, expected); e != nil {
			if fatal(e) {
				return e
			}
			if firstError == nil {
				firstError = e
			}
		}
		if i%20 == 0 {
			log(fmt.Sprintf("Обновление метаданных: %d / %d кластеров", i+1, len(clusters)))
		}
	}
	return firstError
}
func watchBackup(ctx context.Context, s *Store, cookies string, workers int, interval time.Duration, log func(string)) error {
	for {
		log("Фоновая проверка недостающих оригиналов…")
		e := runMode(ctx, s, cookies, workers, true, log)
		if e != nil {
			if fatal(e) {
				return e
			}
			log(e.Error())
		}
		st, se := s.stats()
		if se != nil {
			return se
		}
		if st.NoOriginal == 0 && st.NoPreview == 0 && st.NoThumbnail == 0 {
			log("Все известные файлы имеют три версии; проверка завершена.")
			return nil
		}
		log("Следующая проверка: " + time.Now().Add(interval).Format(time.RFC3339))
		if e := waitContext(ctx, interval); e != nil {
			return e
		}
	}
}
