package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	defaultConfigPath = "data/config.json"
	pollInterval      = 60 * time.Second
	timeLayout        = "2006-01-02 15:04"
)

type Config struct {
	Prosessing  string `json:"prosessing"`
	Results     string `json:"results"`
	ResultsPath string `json:"results_path"`
	Path        string `json:"path"`
	Logs        string `json:"logs"`
}

type Record struct {
	Model   string `json:"model"`
	Poligon string `json:"poligon"`
	Cloud   *int   `json:"cloud,omitempty"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Time    string `json:"time"`
	Start2  string `json:"start2,omitempty"`
	End2    string `json:"end2,omitempty"`
}

type QueueItem struct {
	ID     string
	Record Record
	Parsed time.Time
}

func main() {
	configPath := flag.String("config", defaultConfigPath, "path to config.json")
	runFlag := flag.Bool("run", false, "run task service")
	flag.Parse()

	if !*runFlag {
		printGreeting(*configPath)
		return
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logger, logFile, err := newLogger(cfg.Logs)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer logFile.Close()

	logger.Printf("task started: config=%s prosessing=%s results=%s results_path=%s path=%s logs=%s", *configPath, cfg.Prosessing, cfg.Results, cfg.ResultsPath, cfg.Path, cfg.Logs)

	sentinelPath := filepath.Join(cfg.Path, "sentinel")
	if _, err := os.Stat(sentinelPath); err != nil {
		logger.Printf("sentinel executable check failed: %v", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for {
		queue, err := loadMap(cfg.Prosessing)
		if err != nil {
			logger.Printf("failed to read prosessing file: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		item, ok := oldest(queue)
		if !ok {
			time.Sleep(pollInterval)
			continue
		}

		r := item.Record
		logger.Printf("picked process: id=%s model=%s poligon=%s cloud=%s start=%s end=%s start2=%s end2=%s time=%s queue=%d",
			item.ID, r.Model, r.Poligon, cloudString(r.Cloud), r.Start, r.End, r.Start2, r.End2, r.Time, len(queue))

		exitCode, runErr := runSentinel(logger, sentinelPath, item)

		// Reload queue before write to avoid losing external updates.
		queueNow, err := loadMap(cfg.Prosessing)
		if err != nil {
			logger.Printf("failed to reload prosessing file after sentinel run: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		if _, exists := queueNow[item.ID]; exists {
			delete(queueNow, item.ID)
			if err := writeMap(cfg.Prosessing, queueNow); err != nil {
				logger.Printf("failed to update prosessing file: %v", err)
				time.Sleep(pollInterval)
				continue
			}
			logger.Printf("process removed from prosessing: id=%s remaining=%d", item.ID, len(queueNow))
		} else {
			logger.Printf("process already absent in prosessing when removing: id=%s", item.ID)
		}

		zipPath := filepath.Join(cfg.ResultsPath, item.ID+".zip")
		if _, err := os.Stat(zipPath); err != nil {
			if os.IsNotExist(err) {
				logger.Printf("result annulled: id=%s zip archive not created by sentinel: %s", item.ID, zipPath)
			} else {
				logger.Printf("result annulled: id=%s unable to stat zip archive %s: %v", item.ID, zipPath, err)
			}
			continue
		}

		resultsMap, err := loadMap(cfg.Results)
		if err != nil {
			logger.Printf("failed to read results file: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		result := item.Record
		result.Time = time.Now().Format(timeLayout)
		resultsMap[item.ID] = result
		if err := writeMap(cfg.Results, resultsMap); err != nil {
			logger.Printf("failed to write results file: %v", err)
			time.Sleep(pollInterval)
			continue
		}
		logger.Printf("result saved: id=%s model=%s poligon=%s cloud=%s start=%s end=%s start2=%s end2=%s time=%s zip=%s total_results=%d sentinel_exit=%d sentinel_error=%v",
			item.ID, result.Model, result.Poligon, cloudString(result.Cloud), result.Start, result.End, result.Start2, result.End2, result.Time, zipPath, len(resultsMap), exitCode, runErr)
	}
}

func printGreeting(configPath string) {
	fmt.Println("Аналитическая Система Интеллектуального Мониторинга")
	fmt.Println("Модуль запуска задач по очереди процессов")
	fmt.Println("ГБУ ПК \"Центр информационного развития Пермского края\". 2026 год\n")
	fmt.Println("Запуск сервиса:")
	fmt.Println("./task --run [--config ./data/config.json]\n")
	fmt.Println("Интервал проверки очереди: 60 секунд")
}
func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if strings.TrimSpace(cfg.Prosessing) == "" {
		return cfg, errors.New("config field prosessing is empty")
	}
	if strings.TrimSpace(cfg.Results) == "" {
		return cfg, errors.New("config field results is empty")
	}
	if strings.TrimSpace(cfg.ResultsPath) == "" {
		return cfg, errors.New("config field results_path is empty")
	}
	if strings.TrimSpace(cfg.Path) == "" {
		return cfg, errors.New("config field path is empty")
	}
	if strings.TrimSpace(cfg.Logs) == "" {
		return cfg, errors.New("config field logs is empty")
	}
	return cfg, nil
}

func newLogger(logDir string) (*log.Logger, *os.File, error) {
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("mkdir logs: %w", err)
	}
	logPath := filepath.Join(logDir, "task.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("open task.log: %w", err)
	}
	return log.New(f, "", log.LstdFlags|log.Lmicroseconds), f, nil
}

func loadMap(path string) (map[string]Record, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]Record{}, nil
		}
		return nil, err
	}
	if strings.TrimSpace(string(data)) == "" {
		return map[string]Record{}, nil
	}
	var m map[string]Record
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if m == nil {
		return map[string]Record{}, nil
	}
	return m, nil
}

func writeMap(path string, m map[string]Record) error {
	if m == nil {
		m = map[string]Record{}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func oldest(m map[string]Record) (QueueItem, bool) {
	if len(m) == 0 {
		return QueueItem{}, false
	}
	items := make([]QueueItem, 0, len(m))
	for id, rec := range m {
		items = append(items, QueueItem{ID: id, Record: rec, Parsed: parseTime(rec.Time)})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Parsed.Equal(items[j].Parsed) {
			return items[i].ID < items[j].ID
		}
		return items[i].Parsed.Before(items[j].Parsed)
	})
	return items[0], true
}

func runSentinel(logger *log.Logger, sentinelPath string, item QueueItem) (int, error) {
	r := item.Record
	args := []string{
		"--model", r.Model,
		"--start", r.Start,
		"--end", r.End,
		"--poligon", r.Poligon,
		"--out", item.ID,
	}
	if r.Cloud != nil {
		args = append(args, "--cloud", fmt.Sprintf("%d", *r.Cloud))
	}
	if strings.TrimSpace(r.Start2) != "" && strings.TrimSpace(r.End2) != "" {
		args = append(args, "--start2", r.Start2, "--end2", r.End2)
	}

	logger.Printf("starting sentinel: %s %s", sentinelPath, strings.Join(args, " "))
	cmd := exec.Command(sentinelPath, args...)
	output, err := cmd.CombinedOutput()
	outText := strings.TrimSpace(string(output))
	if outText == "" {
		logger.Printf("sentinel output for id=%s: <empty>", item.ID)
	} else {
		logger.Printf("sentinel output for id=%s:\n%s", item.ID, outText)
	}
	if err != nil {
		code := exitStatus(err)
		logger.Printf("sentinel finished with error: id=%s exit_code=%d error=%v", item.ID, code, err)
		return code, err
	}
	logger.Printf("sentinel finished successfully: id=%s exit_code=0", item.ID)
	return 0, nil
}

func parseTime(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Unix(0, 0)
	}
	layouts := []string{
		timeLayout,
		"2006-01-02 15:04:05",
		time.RFC3339,
		"2006-01-02T15:04",
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t
		}
	}
	return time.Unix(0, 0)
}

func cloudString(v *int) string {
	if v == nil {
		return "<unset>"
	}
	return fmt.Sprintf("%d", *v)
}

func exitStatus(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		if ws, ok := ee.Sys().(syscall.WaitStatus); ok {
			return ws.ExitStatus()
		}
		return 1
	}
	return 1
}
