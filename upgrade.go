package main

import (
	"context"
	"fmt"
	"strconv"
)

func (c *Client) refreshKnown(ctx context.Context, s *Store, log func(string)) error {
	rows, e := s.DB.Query(`SELECT DISTINCT cluster FROM files f WHERE ` + confirmedSQL("f") + ` AND (NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='original') OR NOT EXISTS(SELECT 1 FROM versions WHERE file_id=f.id AND quality='preview'))`)
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
