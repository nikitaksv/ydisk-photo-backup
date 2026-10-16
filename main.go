package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"text/tabwriter"
)

func main() {
	if e := entry(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func entry() error {
	command := "serve"
	args := os.Args[1:]
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		command = args[0]
		args = args[1:]
	}
	defaultOut := "backup"
	if runtime.GOOS == "windows" {
		if _, e := os.Stat(`D:\`); e == nil {
			defaultOut = `D:\YandexPhotoBackup`
		}
	}
	fs := flag.NewFlagSet("photoslice "+command, flag.ContinueOnError)
	out := fs.String("out", defaultOut, "папка архива")
	cookies := fs.String("cookies", "", "путь к cookies.json (по умолчанию в папке архива)")
	workers := fs.Int("workers", 12, "параллельных загрузок, 1..32")
	zipSize := fs.Int("batch-size", 500, "фотографий в серверном ZIP, 1..500")
	zipLimit := fs.Int("max-batches", 0, "ограничить ZIP-пробу числом пачек; 0 — все пачки")
	zipEnabled := fs.Bool("zip-enabled", false, "серверные ZIP до 500 файлов; по умолчанию выключены")
	filter := fs.String("filter", "all", "all, saved, confirmed, unconfirmed, records-all, missing-original, missing-preview, missing-thumbnail, no-copy, failed, pending, preview-best, thumbnail-best")
	limit := fs.Int("limit", 50, "файлов на странице списка (1..100)")
	offset := fs.Int("offset", 0, "смещение списка")
	asJSON := fs.Bool("json", false, "статистика / список в JSON")
	listen := fs.String("listen", "127.0.0.1:8789", "адрес локального интерфейса")
	if e := fs.Parse(args); e != nil {
		return e
	}
	if *workers < 1 || *workers > 32 {
		return fmt.Errorf("workers: от 1 до 32")
	}
	if *zipSize < 1 || *zipSize > 500 || *zipLimit < 0 {
		return fmt.Errorf("batch-size: 1..500; max-batches: не меньше 0")
	}
	root, e := filepath.Abs(*out)
	if e != nil {
		return e
	}
	if filepath.Dir(root) == root {
		return fmt.Errorf("выбери отдельную папку, а не корень диска")
	}
	if *cookies == "" {
		*cookies = filepath.Join(root, "cookies.json")
	}
	s, e := openStore(root)
	if e != nil {
		return e
	}
	defer s.DB.Close()
	var settingsErr error
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "zip-enabled" {
			settingsErr = s.set("zip_enabled", fmt.Sprint(*zipEnabled))
		}
	})
	if settingsErr != nil {
		return settingsErr
	}
	if command == "zip" && s.get("zip_enabled") != "true" {
		return fmt.Errorf("серверный ZIP выключен; включи -zip-enabled=true")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	log := func(msg string) { fmt.Println(msg) }
	switch command {
	case "serve":
		return serve(ctx, s, *cookies, *workers, *listen)
	case "run":
		e := runBackup(ctx, s, *cookies, *workers, log)
		if re := s.report(); re != nil && e == nil {
			e = re
		}
		return e
	case "resume", "reconcile":
		e := runSelected(ctx, s, *cookies, *workers, command, log, nil)
		if re := s.report(); re != nil && e == nil {
			e = re
		}
		return e
	case "zip":
		mode := "zip"
		if *zipLimit > 0 {
			mode = "zip-test"
		}
		e := runSelected(ctx, s, *cookies, *workers, mode, log, func(c *Client) { c.ZIPBatchSize = *zipSize; c.ZIPMaxBatches = *zipLimit })
		if re := s.report(); re != nil && e == nil {
			e = re
		}
		return e
	case "upgrade":
		e := runMode(ctx, s, *cookies, *workers, true, log)
		if re := s.report(); e == nil {
			e = re
		}
		return e
	case "watch":
		return runSelected(ctx, s, *cookies, *workers, "resume", log, nil)
	case "status", "stats":
		st, e := s.stats()
		if e != nil {
			return e
		}
		if *asJSON {
			return json.NewEncoder(os.Stdout).Encode(st)
		}
		fmt.Printf("Сохранённые файлы: %d; подтверждённые без копии: %d; неподтверждённые записи: %d\n", st.Done, st.ConfirmedNoCopy, st.Unconfirmed)
		fmt.Printf("Подтверждено файлов: %d; всего записей: %d; архивов версий: %d; %.2f ГБ\n", st.Confirmed, st.Records, st.Archives, float64(st.Bytes)/1e9)
		fmt.Printf("Лучшее качество: оригинал %d | превью %d | миниатюра %d | без копии %d\n", st.Original, st.OnlyPreview, st.OnlyThumbnail, st.ConfirmedNoCopy)
		fmt.Printf("                 Есть      Нет\nОригинал       %7d  %7d\nПревью         %7d  %7d\nМиниатюра      %7d  %7d\n", st.Original, st.NoOriginal, st.Preview, st.NoPreview, st.Thumbnail, st.NoThumbnail)
		fmt.Printf("Список: %s; ошибки без копии: %d; ожидают: %d\n", st.Inventory, st.Failed, st.Pending)
		fmt.Printf("Физических ZIP: %d; пакетных ZIP: %d\n", st.ArchiveFiles, st.ZIPArchives)
		z := s.zipStatus()
		if z.Stage != "" || z.Queued > 0 {
			fmt.Printf("Очередь пакетного ZIP: %d оригиналов; пачки до 500, без одиночных ZIP-запросов.\n", z.Queued)
			fmt.Printf("ZIP: %s; пачка %d/%d; оригиналов %d/%d; отдельных проверок %d\n", z.Stage, z.Batch, z.Batches, z.Imported, z.Requested, z.Missing)
		}
		if *filter != "all" {
			_, n, e := s.list(*filter, 0, 1)
			if e != nil {
				return e
			}
			fmt.Printf("Фильтр %s: %d файлов\n", *filter, n)
		}
		return nil
	case "list":
		rows, n, e := s.list(*filter, *offset, *limit)
		if e != nil {
			return e
		}
		if *asJSON {
			return json.NewEncoder(os.Stdout).Encode(Object{"total": n, "files": rows})
		}
		fmt.Printf("Фильтр: %s; всего: %d\n", *filter, n)
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "Учёт\tФайл\tОригинал\tПревью\tМиниатюра\tДата создания\tID")
		for _, r := range rows {
			mark := func(v bool) string {
				if v {
					return "есть"
				}
				return "—"
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.RecordState, r.Name, mark(r.Original), mark(r.Preview), mark(r.Thumbnail), r.Created, r.ID)
		}
		return w.Flush()
	case "verify":
		unlock, e := lockRoot(root)
		if e != nil {
			return e
		}
		defer unlock()
		if e := verifySaved(ctx, s, log); e != nil {
			return e
		}
		return s.report()
	case "report":
		return s.report()
	default:
		return fmt.Errorf("команды: serve, run, resume, reconcile, zip, upgrade, watch, stats, list, verify, report")
	}
}
