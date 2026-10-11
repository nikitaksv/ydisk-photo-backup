package main

import (
	"context"
	"errors"
	"fmt"
)

func (c *Client) collect(ctx context.Context, s *Store, log func(string)) error {
	if e := s.set("inventory", "incomplete"); e != nil {
		return e
	}
	init, e := c.call(ctx, "intapi/photo-init-snapshot", nil)
	if e != nil {
		return e
	}
	psid := first(init, "photoslice_id", "photosliceId", "id")
	if psid == "" {
		return errors.New("API не вернул photoslice_id")
	}
	seen := map[string]bool{}
	clusters := []Object{}
	var problems []string
	const amount = 500
	for offset := 0; ; {
		page, e := c.call(ctx, "intapi/photo-get-snapshot", Object{"id": psid, "locale": "ru", "amount": amount, "offset": offset})
		if e != nil {
			if fatal(e) {
				return e
			}
			problems = append(problems, e.Error())
			break
		}
		if _, ok := page["items"]; !ok {
			problems = append(problems, "неизвестная структура списка кластеров: items отсутствует")
			break
		}
		items, valid := page["items"].([]any)
		if !valid {
			problems = append(problems, "items должен быть массивом")
			break
		}
		added := 0
		for _, v := range items {
			m := obj(v)
			id := first(m, "id", "cluster_id")
			if id == "" {
				problems = append(problems, "кластер без id")
				continue
			}
			if !seen[id] {
				m["id"] = id
				clusters = append(clusters, m)
				seen[id] = true
				added++
			}
		}
		if len(items) == 0 {
			break
		}
		if added == 0 {
			problems = append(problems, "API повторяет страницу кластеров: полный список не подтверждён")
			break
		}
		offset += len(items)
		total := num(page["total"])
		if total > 0 && int64(offset) >= total {
			break
		}
		// Request the next page even when it is shorter than requested: the API may
		// silently cap the page size. Stop only on an empty page or explicit total.
		if offset > 1000000 {
			problems = append(problems, "слишком много кластеров: требуется проверить формат API")
			break
		}
	}
	log(fmt.Sprintf("Найдено дней/кластеров: %d", len(clusters)))
	for i, cl := range clusters {
		if e := ctx.Err(); e != nil {
			return e
		}
		id := str(cl["id"])
		expected := num(cl["size"])
		if e := s.set("cluster_size:"+id, fmt.Sprint(expected)); e != nil {
			return &LocalError{e}
		}
		e := c.collectCluster(ctx, s, psid, id, expected)
		if e != nil {
			if fatal(e) || ctx.Err() != nil {
				return e
			}
			problems = append(problems, fmt.Sprintf("кластер %s: %v", id, e))
			log(fmt.Sprintf("Не удалось полностью прочитать кластер %d: %v", i+1, e))
		}
		if i%10 == 0 || i == len(clusters)-1 {
			log(fmt.Sprintf("Список: %d / %d кластеров", i+1, len(clusters)))
		}
	}
	if len(problems) > 0 {
		if e := s.set("inventory_error", fmt.Sprintf("Ошибок обхода: %d; %s", len(problems), problems[0])); e != nil {
			return &LocalError{e}
		}
		return fmt.Errorf("полнота списка не подтверждена (ошибок: %d); известные файлы будут скачаны; повтори запуск", len(problems))
	}
	if e := s.set("inventory_error", ""); e != nil {
		return e
	}
	return s.set("inventory", "complete")
}
func (c *Client) collectCluster(ctx context.Context, s *Store, psid, cid string, expected int64) error {
	const pageSize = 200
	seen := map[string]bool{}
	for offset := 0; ; {
		end := offset + pageSize - 1
		if expected > 0 && int64(end) >= expected {
			end = int(expected) - 1
		}
		if end < offset {
			break
		}
		d, e := c.call(ctx, "intapi/photo-get-clusters-with-resources", Object{"photosliceId": psid, "clusters": Object{cid: Object{"range": []int{offset, end}}}, "hideScreenshots": false})
		if e != nil {
			return e
		}
		resources := obj(d["resources"])
		clFetched := arr(obj(d["clusters"])["fetched"])
		// Some versions return item metadata nested in clusters; others return
		// references in clusters plus the actual metadata in resources.fetched.
		byAlias := map[string]Object{}
		all := []any{}
		for _, v := range arr(resources["fetched"]) {
			m := obj(v)
			all = append(all, v)
			for _, key := range []string{"path", "id", "resource_id"} {
				if alias := str(m[key]); alias != "" {
					byAlias[alias] = m
				}
			}
		}
		for _, v := range arr(resources["missing"]) {
			all = append(all, v)
		}
		indexed := 0
		for _, v := range clFetched {
			cl := obj(v)
			if id := first(cl, "id", "cluster_id"); id != "" && id != cid {
				continue
			}
			for _, item := range arr(cl["items"]) {
				indexed++
				if ref := str(item); ref != "" {
					if full := byAlias[ref]; full != nil {
						all = append(all, full)
					} else {
						all = append(all, Object{"path": ref})
					}
				} else {
					m := obj(item)
					ref := first(m, "path", "id", "resource_id")
					if full := byAlias[ref]; full != nil {
						merged := Object{}
						for k, v := range m {
							merged[k] = v
						}
						for k, v := range full {
							merged[k] = v
						}
						all = append(all, merged)
					} else {
						all = append(all, m)
					}
				}
			}
		}
		if len(clFetched) == 0 && resources["fetched"] == nil && resources["missing"] == nil {
			return errors.New("API не вернул clusters.fetched или resources")
		}
		pageSeen := map[string]Resource{}
		for _, v := range all {
			r, e := resource(v, cid)
			if e != nil {
				return e
			}
			old, ok := pageSeen[r.ID]
			if !ok || len(r.Raw) > len(old.Raw) {
				pageSeen[r.ID] = r
			}
		}
		added := 0
		toSave := make([]Resource, 0, len(pageSeen))
		for id, r := range pageSeen {
			if !seen[id] {
				added++
			}
			seen[id] = true
			toSave = append(toSave, r)
		}
		if e := s.saveMany(toSave); e != nil {
			return &LocalError{e}
		}
		count := indexed
		if count == 0 {
			count = len(pageSeen)
		}
		if count == 0 {
			break
		}
		if added == 0 {
			return errors.New("API повторяет страницу ресурсов")
		}
		offset += count
		if expected > 0 && int64(offset) >= expected {
			break
		}
		if offset > 1000000 {
			return errors.New("лимит ресурсов кластера")
		}
	}
	if expected > 0 && int64(len(seen)) < expected {
		return fmt.Errorf("ожидалось %d ресурсов, учтено %d", expected, len(seen))
	}
	return nil
}
