package main

import (
	"context"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

//go:embed ui/*
var ui embed.FS

const buildVersion = "2026-10-11.tui.2"

type Job struct {
	mu             sync.Mutex
	running        bool
	cancel         context.CancelFunc
	phase, message string
	logs           []string
	done           chan struct{}
	watching       bool
	interval       time.Duration
	next           time.Time
	client         *Client
}

type ZIPOptions struct {
	BatchSize  int `json:"batch_size"`
	MaxBatches int `json:"max_batches"`
}

func (j *Job) log(msg string) {
	fmt.Println(msg)
	j.mu.Lock()
	defer j.mu.Unlock()
	j.logs = append(j.logs, time.Now().Format("15:04:05")+"  "+msg)
	if len(j.logs) > 200 {
		j.logs = j.logs[len(j.logs)-200:]
	}
}
func serve(ctx context.Context, s *Store, cookies string, workers int, listen string) error {
	ctx, shutdown := context.WithCancel(ctx)
	defer shutdown()
	host, _, e := net.SplitHostPort(listen)
	if e != nil {
		return e
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("разрешён только loopback")
	}
	key := make([]byte, 32)
	if _, e := rand.Read(key); e != nil {
		return e
	}
	token := hex.EncodeToString(key)
	interval, e := time.ParseDuration(s.get("watch_interval"))
	if e != nil || interval < time.Minute {
		interval = 6 * time.Hour
	}
	next, _ := time.Parse(time.RFC3339, s.get("watch_next"))
	if next.IsZero() {
		next = time.Now().Add(interval)
	}
	job := &Job{phase: "idle", message: "Выбери cookies.json, затем запусти архивирование.", watching: s.get("watch_enabled") == "true", interval: interval, next: next}
	metrics := &Telemetry{}
	go metrics.run(ctx, s, job)
	go metrics.runStorage(ctx, s.Root)
	mux := http.NewServeMux()
	writeJSON := func(w http.ResponseWriter, v any) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		json.NewEncoder(w).Encode(v)
	}
	launch := func(mode string, zipConfig ...ZIPOptions) error {
		options := ZIPOptions{BatchSize: 500}
		if mode == "zip-test" {
			options = ZIPOptions{BatchSize: 20, MaxBatches: 1}
		}
		if len(zipConfig) > 0 {
			options = zipConfig[0]
		}
		job.mu.Lock()
		if job.running {
			job.mu.Unlock()
			return errors.New("работа уже идёт")
		}
		if _, e := os.Stat(cookies); e != nil {
			job.mu.Unlock()
			return errors.New("сначала загрузи cookies.json")
		}
		if mode == "zip" {
			if e := s.set("watch_enabled", "true"); e != nil {
				job.mu.Unlock()
				return errors.New("не удалось включить периодическую проверку")
			}
			job.watching = true
			job.next = time.Now().Add(job.interval)
		}
		workCtx, cancel := context.WithCancel(ctx)
		job.cancel = cancel
		job.running = true
		job.client = nil
		job.done = make(chan struct{})
		finished := job.done
		job.phase = "running"
		if mode == "upgrade" {
			job.phase = "upgrading"
		}
		if mode == "check" {
			job.phase = "checking"
		}
		if strings.HasPrefix(mode, "zip") {
			job.phase = "zipping"
		}
		job.message = "Работа идёт. Каждая версия сохраняется отдельно."
		job.mu.Unlock()
		go func() {
			defer close(finished)
			defer cancel()
			var e error
			if mode == "check" {
				var c *Client
				c, e = newClient(cookies)
				if e == nil {
					e = c.auth(workCtx)
					c.HTTP.CloseIdleConnections()
				}
			} else {
				e = runSelected(workCtx, s, cookies, workers, mode, job.log, func(c *Client) {
					if strings.HasPrefix(mode, "zip") {
						c.ZIPBatchSize = options.BatchSize
						c.ZIPMaxBatches = options.MaxBatches
					}
					job.mu.Lock()
					job.client = c
					job.mu.Unlock()
				})
				if re := s.report(); re != nil && e == nil {
					e = re
				}
			}
			st, _ := s.stats()
			job.mu.Lock()
			defer job.mu.Unlock()
			job.running = false
			job.cancel = nil
			if e == nil {
				job.phase = "completed"
				if mode == "check" {
					job.message = "Вход подтверждён."
				} else {
					job.message = fmt.Sprintf("Сохранено файлов: %d / %d. Без оригинала: %d. Полученные версии сохранены отдельно.", st.Done, st.Total, st.NoOriginal)
				}
				if mode == "zip-test" {
					z := s.zipStatus()
					job.message = fmt.Sprintf("ZIP-проба: %d оригиналов из %d, отдельных проверок %d. %.1f с. Полный проход можно запустить отдельно.", z.Imported, z.Requested, z.Missing, z.Seconds)
				}
				if mode == "zip" {
					z := s.zipStatus()
					job.message = fmt.Sprintf("ZIP-проход завершён: %d архивов, %d проверенных оригиналов. %d пропусков оставлены периодической проверке; полный архив аккаунта ещё не подтверждён.", z.Archives, z.Imported, z.Missing)
				}
			} else if errors.Is(e, context.Canceled) {
				job.phase = "stopped"
				job.message = "Остановлено. Готовые версии сохранены."
			} else {
				job.phase = "attention"
				job.message = e.Error()
			}
			if job.watching {
				if e != nil && fatal(e) && !errors.Is(e, context.Canceled) {
					job.watching = false
					s.set("watch_enabled", "false")
					job.message += " Фоновая проверка приостановлена; устрани причину и включи её снова."
				} else if mode != "check" && st.Total > 0 && st.NoOriginal == 0 && st.NoPreview == 0 && st.NoThumbnail == 0 {
					job.watching = false
					s.set("watch_enabled", "false")
					job.message += " Все три версии получены; периодическая проверка завершена."
				} else {
					job.next = time.Now().Add(job.interval)
					s.set("watch_next", job.next.Format(time.RFC3339))
				}
			}
		}()
		return nil
	}
	mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method", 405)
			return
		}
		st, e := s.stats()
		if e != nil {
			http.Error(w, "ошибка БД", 500)
			return
		}
		_, ce := os.Stat(cookies)
		zipProgress := s.zipStatus()
		transfer, storage := metrics.status()
		job.mu.Lock()
		defer job.mu.Unlock()
		rate := RateStatus{}
		if job.client != nil {
			rate = job.client.rateStatus()
		}
		phase, message := job.phase, job.message
		if phase == "zipping" && (zipProgress.Stage == "completed" || zipProgress.Stage == "deferred") {
			phase = "running"
		}
		if job.running && rate.LimitedUntil != "" {
			phase = "rate_limited"
			message = "Яндекс ограничил запросы. Все новые запросы ждут общей паузы; файлы остаются в очереди."
		}
		writeJSON(w, Object{"build": buildVersion, "transfer": transfer, "storage": storage, "zip": zipProgress, "rate": rate, "stats": st, "running": job.running, "phase": phase, "message": message, "logs": job.logs, "out": s.Root, "cookies": ce == nil, "token": token, "watching": job.watching, "watch_interval_minutes": int(job.interval / time.Minute), "next_check": job.next.Format(time.RFC3339)})
	})
	mux.HandleFunc("/api/files", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method", 405)
			return
		}
		offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		files, total, e := s.list(r.URL.Query().Get("filter"), offset, limit)
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		writeJSON(w, Object{"files": files, "total": total})
	})
	mux.HandleFunc("/api/cookies", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		job.mu.Lock()
		defer job.mu.Unlock()
		if job.running {
			http.Error(w, "останови текущую работу", 409)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4<<20)
		if e := r.ParseMultipartForm(4 << 20); e != nil {
			http.Error(w, "нужен JSON-файл", 400)
			return
		}
		if r.MultipartForm != nil {
			defer r.MultipartForm.RemoveAll()
		}
		f, _, e := r.FormFile("cookies")
		if e != nil {
			http.Error(w, "нужен файл cookies", 400)
			return
		}
		defer f.Close()
		b, e := io.ReadAll(f)
		if e != nil {
			http.Error(w, "ошибка чтения", 400)
			return
		}
		a, e := parseCookies(b)
		if e != nil {
			http.Error(w, e.Error(), 400)
			return
		}
		if e := atomicFile(cookies, func(f *os.File) error { return json.NewEncoder(f).Encode(a) }); e != nil {
			http.Error(w, "не удалось сохранить cookies", 500)
			return
		}
		job.message = "Cookies сохранены локально. Проверь вход и запусти архивирование."
		writeJSON(w, Object{"ok": true, "count": len(a)})
	})
	action := func(mode string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				http.Error(w, "method", 405)
				return
			}
			if e := launch(mode); e != nil {
				http.Error(w, e.Error(), 409)
				return
			}
			writeJSON(w, Object{"ok": true})
		}
	}
	mux.HandleFunc("/api/start", action("run"))
	mux.HandleFunc("/api/resume", action("resume"))
	zipAction := func(mode string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "POST" {
				http.Error(w, "method", 405)
				return
			}
			config := ZIPOptions{BatchSize: 500}
			if mode == "zip-test" {
				config = ZIPOptions{BatchSize: 20, MaxBatches: 1}
			}
			r.Body = http.MaxBytesReader(w, r.Body, 4096)
			if e := json.NewDecoder(r.Body).Decode(&config); e != nil && !errors.Is(e, io.EOF) {
				http.Error(w, "неверные настройки ZIP", 400)
				return
			}
			if config.BatchSize < 1 || config.BatchSize > 500 || config.MaxBatches < 0 || mode == "zip-test" && config.MaxBatches == 0 {
				http.Error(w, "ZIP: batch_size 1..500; проба требует max_batches > 0", 400)
				return
			}
			if e := launch(mode, config); e != nil {
				http.Error(w, e.Error(), 409)
				return
			}
			writeJSON(w, Object{"ok": true})
		}
	}
	mux.HandleFunc("/api/zip", zipAction("zip"))
	mux.HandleFunc("/api/zip-test", zipAction("zip-test"))
	mux.HandleFunc("/api/check", action("check"))
	mux.HandleFunc("/api/upgrade", action("upgrade"))
	mux.HandleFunc("/api/rate-tune", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		var config struct {
			Enabled bool `json:"enabled"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if json.NewDecoder(r.Body).Decode(&config) != nil {
			http.Error(w, "неверные настройки", 400)
			return
		}
		job.mu.Lock()
		client, active := job.client, job.running
		job.mu.Unlock()
		if client == nil || !active {
			http.Error(w, "сначала запусти загрузку", 409)
			return
		}
		var e error
		if config.Enabled {
			e = client.startRateTune()
		} else {
			e = client.stopRateTune()
		}
		if e != nil {
			http.Error(w, "не удалось сохранить подбор RPS", 500)
			return
		}
		job.log("Подбор RPS: проверка ступеней по 90 секунд, минимум 100 запросов и 10 успешных ответов. Первый 429 отменяет разгон.")
		writeJSON(w, Object{"ok": true, "rate": client.rateStatus()})
	})
	mux.HandleFunc("/api/rate", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		var config struct {
			IntervalMS         int64 `json:"interval_ms"`
			FallbackIntervalMS int64 `json:"fallback_interval_ms,omitempty"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if e := json.NewDecoder(r.Body).Decode(&config); e != nil || config.IntervalMS < 100 || config.IntervalMS > 5000 || config.FallbackIntervalMS < 0 || config.FallbackIntervalMS > 5000 || config.FallbackIntervalMS != 0 && config.FallbackIntervalMS <= config.IntervalMS {
			http.Error(w, "интервал запросов: 100..5000 мс; интервал возврата должен быть больше исходного", 400)
			return
		}
		job.mu.Lock()
		client, active := job.client, job.running
		job.mu.Unlock()
		if client == nil || !active {
			http.Error(w, "сначала запусти загрузку", 409)
			return
		}
		var fallback []time.Duration
		if config.FallbackIntervalMS > 0 {
			fallback = []time.Duration{time.Duration(config.FallbackIntervalMS) * time.Millisecond}
		}
		if e := client.configureInterval(time.Duration(config.IntervalMS)*time.Millisecond, fallback...); e != nil {
			http.Error(w, "не удалось сохранить интервал", 500)
			return
		}
		job.log(fmt.Sprintf("Базовый интервал всех потоков: %d мс. При повторных 429 ускорение автоматически отменяется.", config.IntervalMS))
		writeJSON(w, Object{"ok": true, "rate": client.rateStatus()})
	})
	mux.HandleFunc("/api/watch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		var config struct {
			Enabled bool `json:"enabled"`
			Minutes int  `json:"interval_minutes"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 4096)
		if e := json.NewDecoder(r.Body).Decode(&config); e != nil {
			http.Error(w, "неверные настройки", 400)
			return
		}
		if config.Minutes < 1 || config.Minutes > 43200 {
			http.Error(w, "интервал: 1..43200 минут", 400)
			return
		}
		st, e := s.stats()
		if e != nil {
			http.Error(w, "ошибка БД", 500)
			return
		}
		if config.Enabled && st.Total == 0 {
			http.Error(w, "сначала собери список и запусти скачивание", 400)
			return
		}
		job.mu.Lock()
		defer job.mu.Unlock()
		job.watching = config.Enabled
		job.interval = time.Duration(config.Minutes) * time.Minute
		job.next = time.Now()
		if e := s.set("watch_enabled", strconv.FormatBool(config.Enabled)); e != nil {
			http.Error(w, "ошибка настроек", 500)
			return
		}
		s.set("watch_interval", job.interval.String())
		s.set("watch_next", job.next.Format(time.RFC3339))
		writeJSON(w, Object{"ok": true})
	})
	mux.HandleFunc("/api/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		job.mu.Lock()
		if job.cancel != nil {
			job.cancel()
		}
		job.mu.Unlock()
		writeJSON(w, Object{"ok": true})
	})
	mux.HandleFunc("/api/shutdown", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method", 405)
			return
		}
		writeJSON(w, Object{"ok": true})
		go shutdown()
	})
	for _, name := range []string{"report.csv", "metadata.jsonl", "summary.json"} {
		name := name
		mux.HandleFunc("/"+name, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Disposition", "attachment; filename="+name)
			http.ServeFile(w, r, s.Root+string(os.PathSeparator)+name)
		})
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Path
		if name == "/" {
			name = "/index.html"
		}
		if name != "/index.html" && name != "/app.js" && name != "/style.css" {
			http.NotFound(w, r)
			return
		}
		b, e := ui.ReadFile("ui" + name)
		if e != nil {
			http.NotFound(w, r)
			return
		}
		switch name {
		case "/app.js":
			w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		case "/style.css":
			w.Header().Set("Content-Type", "text/css; charset=utf-8")
		default:
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
		}
		w.Write(b)
	})
	secure := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != listen {
			http.Error(w, "invalid host", 403)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; frame-ancestors 'none'; connect-src 'self'; base-uri 'none'")
		if strings.HasPrefix(r.URL.Path, "/api/") && r.Method != "GET" {
			if r.Header.Get("X-Backup-Token") != token {
				http.Error(w, "invalid token", 403)
				return
			}
			origin := r.Header.Get("Origin")
			if origin != "" && origin != "http://"+listen {
				http.Error(w, "invalid origin", 403)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
	listener, e := net.Listen("tcp", listen)
	if e != nil {
		return e
	}
	server := &http.Server{Handler: secure, ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	fmt.Println("Photoslice Backup: http://" + listen)
	fmt.Println("Папка архива:", s.Root)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				job.mu.Lock()
				due := job.watching && !job.running && !time.Now().Before(job.next)
				if due {
					job.next = time.Now().Add(job.interval)
				}
				job.mu.Unlock()
				if due {
					if e := launch("upgrade"); e != nil {
						job.log("Фоновая проверка: " + e.Error())
					}
				}
			}
		}
	}()
	go func() {
		<-ctx.Done()
		job.mu.Lock()
		if job.cancel != nil {
			job.cancel()
		}
		job.mu.Unlock()
		shut, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		server.Shutdown(shut)
	}()
	e = server.Serve(listener)
	job.mu.Lock()
	if job.cancel != nil {
		job.cancel()
	}
	finished := job.done
	job.mu.Unlock()
	if finished != nil {
		<-finished
	}
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}
