package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func printUsage(configPath string) {
	fmt.Fprintln(os.Stderr, "ГБУ ПК \"Центр информационного развития Пермского края\". 2026 год")
	fmt.Fprintln(os.Stderr, "Модуль поиска объектов по спутниковым снимкам Sentinel2 (L1C и L2A)\n")
	fmt.Fprintf(os.Stderr, "Пример: ./sentinel --config %s --model hogweed --start 2025-06-11 --end 2025-06-20 --out run1\n", configPath)
	fmt.Fprintln(os.Stderr, "Для моделей с inputs=2 дополнительно задаются --start2 и --end2")
}

func initLogger(logDir string) (ioWriteCloser, error) {
	if logDir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(logDir, 0o755); err != nil {
		return nil, err
	}
	fp := filepath.Join(logDir, "asim.log")
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	mw := NewMultiWriteCloser(os.Stdout, f)
	log.SetOutput(mw)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return mw, nil
}

type multiWriteCloser struct {
	writers []ioWriteCloser
}

type ioWriteCloser interface {
	Write([]byte) (int, error)
	Close() error
}

type nopWriteCloser struct{ *os.File }

func (n nopWriteCloser) Close() error { return nil }

func NewMultiWriteCloser(stdout *os.File, file *os.File) *multiWriteCloser {
	return &multiWriteCloser{writers: []ioWriteCloser{nopWriteCloser{stdout}, file}}
}

func (m *multiWriteCloser) Write(p []byte) (int, error) {
	for _, w := range m.writers {
		if _, err := w.Write(p); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

func (m *multiWriteCloser) Close() error {
	var first error
	for _, w := range m.writers {
		if err := w.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func main() {
	configPath := flag.String("config", DefaultConfigPath(), "Путь к config.json")
	cashPath := flag.String("cash", "", "Путь к cash.json или каталогу Sentinel с годами")
	modelName := flag.String("model", "hogweed", "Название модели из config.json")
	outputName := flag.String("out", "", "Имя выходного архива zip без пути")
	minAreaFlag := flag.Float64("min-area", 0, "Минимальная площадь полигона (если 0, берется из модели)")
	simplifyFlag := flag.Float64("simplify", 0, "Упрощение геометрии (если 0, берется из модели)")
	device := flag.String("device", "cuda", "Тип расчетов: cpu|cuda")
	cudaID := flag.Int("cuda-device", 0, "Номер GPU для ONNX Runtime")
	batch := flag.Int("batch", 4, "Сколько тайлов обрабатывается за один вызов модели")
	start := flag.String("start", "", "Начало периода YYYY-MM-DD")
	end := flag.String("end", "", "Конец периода YYYY-MM-DD")
	start2 := flag.String("start2", "", "Начало второго периода YYYY-MM-DD")
	end2 := flag.String("end2", "", "Конец второго периода YYYY-MM-DD")
	cloud := flag.Float64("cloud", 50, "Максимальная облачность тайла в процентах (в анализ попадают снимки с облачностью <= этого значения)")
	poligon := flag.String("poligon", "kray", "Идентификатор полигона поиска из config.json")
	dumpPatchDir := flag.String("dump-patch", "", "Каталог для сохранения диагностического дампа первого патча (вход+выход модели)")
	flag.Parse()

	// Устанавливаем каталог дампа (глобальная переменная из dump.go).
	if strings.TrimSpace(*dumpPatchDir) != "" {
		dumpDir = strings.TrimSpace(*dumpPatchDir)
	}

	cfg, err := LoadAppConfig(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка загрузки config.json:", err)
		printUsage(*configPath)
		os.Exit(1)
	}
	SetModelSpecs(cfg.Models)

	logFile, err := initLogger(cfg.Logs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка инициализации лога:", err)
		os.Exit(1)
	}
	if logFile != nil {
		defer logFile.Close()
	}

	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(*cashPath)), ".json") {
		if _, err := os.Stat(*cashPath); err != nil {
			msg := fmt.Sprintf("cash.json not found: %s", *cashPath)
			log.Printf("startup failed: %s", msg)
			fmt.Fprintln(os.Stderr, msg)
			os.Exit(1)
		}
	}

	if strings.TrimSpace(*start) == "" || strings.TrimSpace(*end) == "" {
		printUsage(*configPath)
		fmt.Fprintln(os.Stderr, "Нужно задать --start и --end")
		os.Exit(2)
	}

	dev := strings.ToLower(strings.TrimSpace(*device))
	if dev != "cpu" && dev != "cuda" && dev != "gpu" {
		fmt.Fprintln(os.Stderr, "Укажите --device cpu или --device cuda")
		os.Exit(2)
	}
	if dev == "gpu" {
		dev = "cuda"
	}

	spec, ok := GetModelSpec(strings.ToLower(strings.TrimSpace(*modelName)))
	if !ok {
		fmt.Fprintln(os.Stderr, "Неизвестная модель. Доступные модели:", strings.Join(ListModelNames(), ", "))
		os.Exit(2)
	}
	if strings.TrimSpace(*cashPath) == "" {
		if strings.EqualFold(spec.Preprocess, "sentinel2a") && strings.TrimSpace(cfg.Sentinel2A) != "" {
			*cashPath = cfg.Sentinel2A
		} else {
			*cashPath = cfg.Sentinel
		}
	}

	if spec.Inputs > 1 && (*start2 == "" || *end2 == "") {
		fmt.Fprintf(os.Stderr, "Модель %s требует --start2 и --end2\n", spec.Name)
		os.Exit(2)
	}
	if *outputName == "" {
		*outputName = spec.Name
	}

	if err := os.MkdirAll(cfg.ResultsPath, 0o755); err != nil {
		log.Printf("mkdir results failed: %v", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.Tmp, 0o755); err != nil {
		log.Printf("mkdir tmp failed: %v", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	runCfg := ProcessConfig{
		App:           cfg,
		Spec:          spec,
		Start:         *start,
		End:           *end,
		Start2:        *start2,
		End2:          *end2,
		Cloud:         *cloud,
		SearchPolygon: *poligon,
		BatchSize:     *batch,
		Device:        dev,
		CudaDeviceID:  *cudaID,
		OutputName:    *outputName,
		CashPath:      *cashPath,
	}
	if *minAreaFlag > 0 {
		runCfg.MinArea = *minAreaFlag
	} else {
		runCfg.MinArea = spec.MinArea
	}
	if *simplifyFlag > 0 {
		runCfg.Simplify = *simplifyFlag
	} else {
		runCfg.Simplify = spec.Simplify
	}

	log.Printf("process started: model=%s output=%s primary=%s..%s secondary=%s..%s cloud=%.2f poligon=%s device=%s batch=%d min_area=%.6f simplify=%.6f cash=%s", spec.Name, runCfg.OutputName, runCfg.Start, runCfg.End, runCfg.Start2, runCfg.End2, runCfg.Cloud, runCfg.SearchPolygon, runCfg.Device, runCfg.BatchSize, runCfg.MinArea, runCfg.Simplify, runCfg.CashPath)
	zipPath, stats, err := RunProcess(runCfg)
	if err != nil {
		log.Printf("processing failed: %v; primary_candidates=%d secondary_candidates=%d footprints_seen=%d tiles_processed=%d tiles_skipped_outside=%d tiles_read_errors=%d model_runs=%d model_errors=%d secondary_misses=%d result_polygons=%d", err, stats.PrimaryCandidates, stats.SecondaryCandidates, stats.TileFootprintsSeen, stats.TilesProcessed, stats.TilesSkippedOutside, stats.TilesReadErrors, stats.ModelRuns, stats.ModelErrors, stats.SecondaryMisses, stats.ResultPolygons)
		log.Printf("process stopped: status=error")
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log.Printf("processing completed: %s; primary_candidates=%d secondary_candidates=%d footprints_seen=%d tiles_processed=%d tiles_skipped_outside=%d tiles_read_errors=%d model_runs=%d model_errors=%d secondary_misses=%d result_polygons=%d", zipPath, stats.PrimaryCandidates, stats.SecondaryCandidates, stats.TileFootprintsSeen, stats.TilesProcessed, stats.TilesSkippedOutside, stats.TilesReadErrors, stats.ModelRuns, stats.ModelErrors, stats.SecondaryMisses, stats.ResultPolygons)
	log.Printf("process stopped: status=ok")
	fmt.Println(zipPath)
}
