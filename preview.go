package main

import (
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var dimensionsRE = regexp.MustCompile(`(?i)(\d{2,5})[x×](\d{2,5})`)

func sizeRank(label string) int {
	switch strings.ToUpper(label) {
	case "ORIGINAL", "ORIG":
		return 100000
	case "XXXL":
		return 2048
	case "XXL":
		return 1280
	case "XL":
		return 1024
	case "L":
		return 800
	case "M":
		return 500
	case "S":
		return 300
	case "XS":
		return 150
	case "XXS":
		return 75
	}
	return 0
}

// Only use URLs actually returned in preview/sizes fields. Signed URLs are never
// fabricated by changing their size parameter.
func previewCandidates(raw Object) []Candidate {
	var out []Candidate
	var walk func(any, string, bool)
	walk = func(v any, label string, enabled bool) {
		switch x := v.(type) {
		case string:
			if !enabled || !(strings.HasPrefix(x, "https://") || strings.HasPrefix(x, "//")) {
				return
			}
			u, _ := url.Parse(x)
			w, h := 0, 0
			rank := sizeRank(label)
			if u != nil {
				sz := u.Query().Get("size")
				if sz != "" {
					if r := sizeRank(sz); r > 0 {
						rank = r
					}
					if m := dimensionsRE.FindStringSubmatch(sz); len(m) > 0 {
						w, _ = strconv.Atoi(m[1])
						h, _ = strconv.Atoi(m[2])
					}
				}
			}
			quality := "preview"
			low := strings.ToLower(label)
			if strings.Contains(low, "thumb") || rank > 0 && rank <= 320 {
				quality = "thumbnail"
			}
			if rank >= 100000 && u != nil && strings.HasPrefix(u.Path, "/disk/") {
				quality = "original"
			}
			if rank == 0 {
				rank = 600
				if quality == "thumbnail" {
					rank = 100
				}
			}
			if w > rank {
				rank = w
			}
			if h > rank {
				rank = h
			}
			out = append(out, Candidate{URL: x, Quality: quality, Width: w, Height: h, Rank: rank})
		case []any:
			for _, v := range x {
				walk(v, label, enabled)
			}
		case map[string]any:
			name := first(x, "name", "size", "label")
			if name == "" {
				name = label
			}
			if enabled {
				if uri := first(x, "url", "href", "src"); uri != "" {
					before := len(out)
					walk(uri, name, true)
					if len(out) > before {
						p := &out[len(out)-1]
						if w := int(num(x["width"])); w > 0 {
							p.Width = w
						}
						if h := int(num(x["height"])); h > 0 {
							p.Height = h
						}
						if p.Width > p.Rank {
							p.Rank = p.Width
						}
						if p.Height > p.Rank {
							p.Rank = p.Height
						}
						if (p.Width > 0 || p.Height > 0) && p.Width <= 320 && p.Height <= 320 && p.Quality != "original" {
							p.Quality = "thumbnail"
						}
					}
				}
			}
			for k, v := range x {
				low := strings.ToLower(k)
				if k == "url" || k == "href" || k == "src" {
					continue
				}
				enable := enabled || strings.Contains(low, "preview") || strings.Contains(low, "thumb") || low == "sizes"
				next := k
				if sizeRank(k) == 0 && enabled {
					next = name
				}
				walk(v, next, enable)
			}
		}
	}
	walk(raw, "", false)
	sort.SliceStable(out, func(i, j int) bool {
		a, b := out[i], out[j]
		tier := func(q string) int {
			switch q {
			case "original":
				return 0
			case "preview":
				return 1
			default:
				return 2
			}
		}
		if tier(a.Quality) != tier(b.Quality) {
			return tier(a.Quality) < tier(b.Quality)
		}
		if a.Width > 0 && a.Height > 0 && b.Width > 0 && b.Height > 0 {
			areaA, areaB := int64(a.Width)*int64(a.Height), int64(b.Width)*int64(b.Height)
			if areaA != areaB {
				return areaA > areaB
			}
		}
		return a.Rank > b.Rank
	})
	seen := map[string]bool{}
	var unique []Candidate
	for _, c := range out {
		if !seen[c.URL] {
			seen[c.URL] = true
			unique = append(unique, c)
		}
	}
	return unique
}
