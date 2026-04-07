package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io/fs"
    "log"
    "math"
    "os"
    "os/exec"
    "path/filepath"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// ─────────────────────────────────────────────────────────────────────────────
// Конфигурация и типы
// ─────────────────────────────────────────────────────────────────────────────

type Config struct {
    Sentinel    string `json:"sentinel"`
    Sentinel2A  string `json:"sentinel2A"`
    ResultsPath string `json:"results_path"`
    Logs        string `json:"logs"`
    Tmp         string `json:"tmp"`
}

// sentinelRoot возвращает путь к каталогу со снимками L2A.
// Поле sentinel2A приоритетно; если пусто — fallback на sentinel.
func (c *Config) sentinelRoot() string {
    if strings.TrimSpace(c.Sentinel2A) != "" {
	return c.Sentinel2A
    }
    return c.Sentinel
}

type TileCache struct {
    Version    int             `json:"version"`
    Preprocess string          `json:"preprocess"`
    Generated  string          `json:"generated"`
    Year       int             `json:"year"`
    Tiles      []SafeTileEntry `json:"tiles"`
}

type SafeTileEntry struct {
    Date          string     `json:"date"`
    CapturedAt    string     `json:"captured_at,omitempty"`
    SceneID       string     `json:"scene_id,omitempty"`
    SafeName      string     `json:"safe_name,omitempty"`
    ImgDataPath   string     `json:"img_data_path"`
    Cloud         float64    `json:"cloud"`
    NodataPercent float64    `json:"nodata_pixel_percentage"`
    Envelope      [4]float64 `json:"envelope"`
    TileID        string     `json:"tile_id"`
}

type bandStats struct {
    Min    float64
    Max    float64
    Mean   float64
    StdDev float64
}

type scaleRange struct {
    SrcMin float64
    SrcMax float64
    DstMin float64
    DstMax float64
}

type tileCandidate struct {
    TileID     string
    Entry      SafeTileEntry
    CapturedAt time.Time
}

type tileSelection struct {
    TileID     string
    Entry      SafeTileEntry
    CapturedAt time.Time
    CloudLimit float64
    Cloud      float64
    SourcePath string
    SourceKind string
    RGBVRT     string
    NormTIF    string
    WarpedTIF  string
    Stats      [3]bandStats
}

// ─────────────────────────────────────────────────────────────────────────────
// Список тайлов Пермского края (не меняется)
// ─────────────────────────────────────────────────────────────────────────────

var tiles = []string{
    "39VWJ", "39VXJ", "40VCP", "40VDP", "40VEP", "40VFP",
    "39VWH", "39VXH", "40VCN", "40VDN", "40VEN", "40VFN",
    "39VWG", "39VXG", "40VCM", "40VDM", "40VEM", "40VFM",
    "39VWF", "39VXF", "40VCL", "40VDL", "40VEL", "40VFL",
    "39VWE", "39VXE", "40VCK", "40VDK", "40VEK", "40VFK",
    "39VWD", "39VXD", "40VCJ", "40VDJ", "40VEJ", "40VFJ",
    "39VWC", "39VXC", "40VCH", "40VDH", "40VEH", "40VFH",
}

// ─────────────────────────────────────────────────────────────────────────────
// Паттерны для поиска каналов R10m в структуре L2A
//
// Sentinel-2 L2A R10m:
//   GRANULE/<tile>/IMG_DATA/R10m/
//     T40VEP_20250601T081531_B04_10m.jp2
//     T40VEP_20250601T081531_B03_10m.jp2
//     T40VEP_20250601T081531_B02_10m.jp2
//     T40VEP_20250601T081531_TCI_10m.jp2
// ─────────────────────────────────────────────────────────────────────────────

var bandPatterns = map[string][]*regexp.Regexp{
    "B04": {
	regexp.MustCompile(`(?i)_B04_10m\.jp2$`),
	regexp.MustCompile(`(?i)_B04\.jp2$`),
    },
    "B03": {
	regexp.MustCompile(`(?i)_B03_10m\.jp2$`),
	regexp.MustCompile(`(?i)_B03\.jp2$`),
    },
    "B02": {
	regexp.MustCompile(`(?i)_B02_10m\.jp2$`),
	regexp.MustCompile(`(?i)_B02\.jp2$`),
    },
}

var tciRegexps = []*regexp.Regexp{
    regexp.MustCompile(`(?i)_TCI_10m\.jp2$`),
    regexp.MustCompile(`(?i)_TCI\.jp2$`),
}

// ─────────────────────────────────────────────────────────────────────────────
// main / run
// ─────────────────────────────────────────────────────────────────────────────

func main() {
    configPath := flag.String("config", filepath.Join("data", "config.json"), "path to config.json")
    startArg := flag.String("start", "", "start date YYYY-MM-DD")
    endArg := flag.String("end", "", "end date YYYY-MM-DD")
    outName := flag.String("out", "", "output file name without extension")
    keepTmp := flag.Bool("keep-tmp", false, "keep temporary working directory")
    targetSRS := flag.String("t-srs", "EPSG:3857", "target projection")
    pixelSize := flag.Float64("tr", 20.0, "target pixel size in map units (default 20m)")
    gdalCacheMB := flag.Int("gdal-cache-mb", 512, "GDAL cache size MB")
    warpMemMB := flag.Int("warp-mem-mb", 512, "gdalwarp working memory MB")
    threads := flag.Int("threads", 4, "number of threads for GDAL")
    flag.Parse()

    if err := run(*configPath, *startArg, *endArg, *outName, *keepTmp,
	*targetSRS, *pixelSize, *gdalCacheMB, *warpMemMB, *threads); err != nil {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
    }
}

func run(configPath, startArg, endArg, outName string, keepTmp bool,
    targetSRS string, pixelSize float64, gdalCacheMB, warpMemMB, threads int) error {

    if strings.TrimSpace(startArg) == "" || strings.TrimSpace(endArg) == "" {
	return errors.New("both --start and --end are required")
    }
    if strings.TrimSpace(outName) == "" {
	return errors.New("--out is required")
    }

    start, err := time.Parse("2006-01-02", startArg)
    if err != nil {
	return fmt.Errorf("invalid --start: %w", err)
    }
    end, err := time.Parse("2006-01-02", endArg)
    if err != nil {
	return fmt.Errorf("invalid --end: %w", err)
    }
    if end.Before(start) {
	return errors.New("--end must be >= --start")
    }

    cfg, err := loadConfig(configPath)
    if err != nil {
	return err
    }

    if err := os.MkdirAll(cfg.Logs, 0o755); err != nil {
	return fmt.Errorf("create logs dir: %w", err)
    }
    logFile, err := os.OpenFile(filepath.Join(cfg.Logs, "mosaic.log"),
	os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil {
	return fmt.Errorf("open mosaic.log: %w", err)
    }
    defer logFile.Close()
    log.SetOutput(logFile)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)

    setGDALEnv(gdalCacheMB, threads)

    log.Printf("mosaic L2A start=%s end=%s out=%s srs=%s tr=%.1f cache=%dMB warp=%dMB threads=%d",
	startArg, endArg, outName, targetSRS, pixelSize, gdalCacheMB, warpMemMB, threads)

    if err := requireExecutables("gdalinfo", "gdalbuildvrt", "gdal_translate", "gdalwarp"); err != nil {
	return err
    }
    for _, d := range []string{cfg.ResultsPath, cfg.Tmp} {
	if err := os.MkdirAll(d, 0o755); err != nil {
	    return fmt.Errorf("mkdir %s: %w", d, err)
	}
    }

    // 1. Выбор снимков: NODATA_PIXEL_PERCENTAGE=0, адаптивный порог облачности
    selections, err := selectScenes(cfg.sentinelRoot(), start, end)
    if err != nil {
	return err
    }
    log.Printf("selected %d tiles", len(selections))

    workDir, err := os.MkdirTemp(cfg.Tmp, "mosaic-*")
    if err != nil {
	return fmt.Errorf("mkdirtemp: %w", err)
    }
    log.Printf("workDir=%s", workDir)

    // 2. Поиск исходных файлов R10m (TCI_10m или B04/B03/B02)
    for i := range selections {
	if err := findTileSource(&selections[i], workDir); err != nil {
	    return err
	}
    }

    // 3. Статистика (быстрый approx через уменьшенную копию)
    for i := range selections {
	stats, err := collectApproxStats(selections[i].SourcePath)
	if err != nil {
	    return fmt.Errorf("stats tile=%s: %w", selections[i].TileID, err)
	}
	selections[i].Stats = stats
	log.Printf("stats tile=%s R[%.0f..%.0f] G[%.0f..%.0f] B[%.0f..%.0f]",
	    selections[i].TileID,
	    stats[0].Min, stats[0].Max,
	    stats[1].Min, stats[1].Max,
	    stats[2].Min, stats[2].Max)
    }

    // 4. Единая глобальная нормализация для бесшовного стыка тайлов
    scale := computeGlobalScale(selections)
    log.Printf("global scale R[%.1f..%.1f] G[%.1f..%.1f] B[%.1f..%.1f]",
	scale[0].SrcMin, scale[0].SrcMax,
	scale[1].SrcMin, scale[1].SrcMax,
	scale[2].SrcMin, scale[2].SrcMax)

    // 5. Нормализация + репроекция каждого тайла
    for i := range selections {
	if err := normalizeTile(workDir, &selections[i], scale); err != nil {
	    return err
	}
	if err := warpTile(workDir, &selections[i], targetSRS, pixelSize, warpMemMB); err != nil {
	    return err
	}
	if !keepTmp {
	    for _, f := range []string{selections[i].NormTIF, selections[i].RGBVRT} {
		if f != "" {
		    _ = os.Remove(f)
		}
	    }
	}
    }

    // 6. Сборка мозаики в JP2 — место, формат и структура пути совпадают с L1C
    year := start.Year()
    yearDir := filepath.Join(cfg.sentinelRoot(), strconv.Itoa(year))
    if err := os.MkdirAll(yearDir, 0o755); err != nil {
	return fmt.Errorf("create year dir: %w", err)
    }
    outPath := filepath.Join(yearDir, outName+".jp2")
    if err := buildMosaic(workDir, selections, outPath); err != nil {
	return err
    }

    log.Printf("mosaic done: %s", outPath)
    fmt.Println(outPath)

    if !keepTmp {
	log.Printf("cleanup: removing workDir=%s", workDir)
	_ = os.RemoveAll(workDir)
	log.Printf("cleanup: done")
    }
    return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Выбор снимков
//
// Алгоритм:
//   1. Из cash.json берём только записи с NODATA_PIXEL_PERCENTAGE == 0.
//   2. Начинаем с порога облачности 1%. Если все тайлы покрыты — готово.
//   3. Если нет — повышаем на 1% (до 100%) пока не покроем все тайлы.
//   4. Для каждого тайла при данном пороге берём самый свежий снимок.
// ─────────────────────────────────────────────────────────────────────────────

func selectScenes(sentinelRoot string, start, end time.Time) ([]tileSelection, error) {
    endInc := end.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

    candidatesByTile := make(map[string][]tileCandidate)
    tileSet := make(map[string]struct{}, len(tiles))
    for _, t := range tiles {
	tileSet[t] = struct{}{}
    }

    for year := start.Year(); year <= end.Year(); year++ {
	cachePath := filepath.Join(sentinelRoot, strconv.Itoa(year), "cash.json")
	cache, err := readCache(cachePath)
	if err != nil {
	    log.Printf("warn: %v", err)
	    continue
	}
	log.Printf("cache year=%d entries=%d", year, len(cache.Tiles))
	for _, entry := range cache.Tiles {
	    tid := normalizeTileID(entry.TileID)
	    if _, ok := tileSet[tid]; !ok {
		continue
	    }
	    capturedAt, ok := parseCapturedAt(entry)
	    if !ok {
		continue
	    }
	    if capturedAt.Before(start) || capturedAt.After(endInc) {
		continue
	    }
	    // Обязательное условие: NODATA_PIXEL_PERCENTAGE должен быть равен 0
	    if entry.NodataPercent != 0 {
		continue
	    }
	    candidatesByTile[tid] = append(candidatesByTile[tid], tileCandidate{
		TileID:     tid,
		Entry:      entry,
		CapturedAt: capturedAt,
	    })
	}
    }

    selected := make(map[string]tileSelection)
    for cloudLimit := 1.0; cloudLimit <= 100.0; cloudLimit += 1.0 {
	var stillMissing []string
	for _, tid := range tiles {
	    if _, ok := selected[tid]; ok {
		continue
	    }
	    best, ok := pickBestCandidate(candidatesByTile[tid], cloudLimit)
	    if !ok {
		stillMissing = append(stillMissing, tid)
		continue
	    }
	    selected[tid] = tileSelection{
		TileID:     tid,
		Entry:      best.Entry,
		CapturedAt: best.CapturedAt,
		CloudLimit: cloudLimit,
		Cloud:      best.Entry.Cloud,
	    }
	    log.Printf("selected tile=%s cloud=%.1f%% limit=%.0f%% date=%s nodata=%.2f%%",
		tid, best.Entry.Cloud, cloudLimit,
		best.CapturedAt.Format("2006-01-02"),
		best.Entry.NodataPercent)
	}
	if len(stillMissing) == 0 {
	    break
	}
	log.Printf("cloud limit %.0f%%: still missing %d tiles: %s",
	    cloudLimit, len(stillMissing), strings.Join(stillMissing, ", "))
    }

    var missing []string
    for _, tid := range tiles {
	if _, ok := selected[tid]; !ok {
	    missing = append(missing, tid)
	}
    }
    if len(missing) > 0 {
	return nil, fmt.Errorf("no scenes found for tiles: %s (period %s..%s)",
	    strings.Join(missing, ", "), start.Format("2006-01-02"), end.Format("2006-01-02"))
    }

    out := make([]tileSelection, 0, len(tiles))
    for _, tid := range tiles {
	out = append(out, selected[tid])
    }
    return out, nil
}

// pickBestCandidate: среди кандидатов с cloud <= limit — самый свежий.
// NODATA_PIXEL_PERCENTAGE уже отфильтрован при сборке candidatesByTile.
func pickBestCandidate(candidates []tileCandidate, cloudLimit float64) (tileCandidate, bool) {
    var eligible []tileCandidate
    for _, c := range candidates {
	if c.Entry.Cloud <= cloudLimit {
	    eligible = append(eligible, c)
	}
    }
    if len(eligible) == 0 {
	return tileCandidate{}, false
    }
    sort.Slice(eligible, func(i, j int) bool {
	return eligible[i].CapturedAt.After(eligible[j].CapturedAt)
    })
    return eligible[0], true
}

// ─────────────────────────────────────────────────────────────────────────────
// Поиск исходных файлов R10m
//
// В L2A каналы 10m лежат в: GRANULE/<tile>/IMG_DATA/R10m/
// Ищем TCI_10m.jp2; если нет — строим VRT из B04/B03/B02.
// ─────────────────────────────────────────────────────────────────────────────

func findTileSource(sel *tileSelection, workDir string) error {
    imgPath := sel.Entry.ImgDataPath
    if imgPath == "" {
	return fmt.Errorf("tile %s: ImgDataPath is empty", sel.TileID)
    }

    tci, err := findFileRecursive(imgPath, tciRegexps)
    if err == nil {
	sel.SourcePath = tci
	sel.SourceKind = "TCI"
	log.Printf("source tile=%s kind=TCI file=%s", sel.TileID, tci)
	return nil
    }

    red, err := findFileRecursive(imgPath, bandPatterns["B04"])
    if err != nil {
	return fmt.Errorf("tile %s: B04 not found in %s: %w", sel.TileID, imgPath, err)
    }
    green, err := findFileRecursive(imgPath, bandPatterns["B03"])
    if err != nil {
	return fmt.Errorf("tile %s: B03 not found: %w", sel.TileID, err)
    }
    blue, err := findFileRecursive(imgPath, bandPatterns["B02"])
    if err != nil {
	return fmt.Errorf("tile %s: B02 not found: %w", sel.TileID, err)
    }

    rgbVRT := filepath.Join(workDir, sel.TileID+"_rgb.vrt")
    if err := runCmd("gdalbuildvrt", "-q", "-separate",
	"-srcnodata", "0", "-vrtnodata", "0",
	rgbVRT, red, green, blue); err != nil {
	return fmt.Errorf("tile %s: build RGB VRT: %w", sel.TileID, err)
    }
    sel.SourcePath = rgbVRT
    sel.SourceKind = "RGB"
    sel.RGBVRT = rgbVRT
    log.Printf("source tile=%s kind=RGB r=%s g=%s b=%s", sel.TileID, red, green, blue)
    return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Файловый кэш (ускоряет работу на NFS)
// ─────────────────────────────────────────────────────────────────────────────

var (
    fileCacheMu sync.Mutex
    fileCache   = map[string][]string{}
)

func listDirFiles(root string) []string {
    fileCacheMu.Lock()
    if files, ok := fileCache[root]; ok {
	fileCacheMu.Unlock()
	return files
    }
    fileCacheMu.Unlock()

    var files []string
    _ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
	if err == nil && !d.IsDir() {
	    files = append(files, path)
	}
	return nil
    })

    fileCacheMu.Lock()
    fileCache[root] = files
    fileCacheMu.Unlock()
    return files
}

func findFileRecursive(root string, patterns []*regexp.Regexp) (string, error) {
    var found []string
    for _, path := range listDirFiles(root) {
	name := filepath.Base(path)
	for _, re := range patterns {
	    if re.MatchString(name) {
		found = append(found, path)
		break
	    }
	}
    }
    if len(found) == 0 {
	return "", fmt.Errorf("no matching file in %s", root)
    }
    sort.Strings(found)
    return found[0], nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Статистика и нормализация
// ─────────────────────────────────────────────────────────────────────────────

func collectApproxStats(sourcePath string) ([3]bandStats, error) {
    var out [3]bandStats

    tmpStat, err := os.CreateTemp("", "mosaic-stat-*.tif")
    if err != nil {
	return out, err
    }
    tmpPath := tmpStat.Name()
    tmpStat.Close()
    defer os.Remove(tmpPath)

    if err := runCmd("gdal_translate",
	"-q",
	"-outsize", "1%", "1%",
	"-of", "GTiff",
	"-ot", "UInt16",
	"-b", "1", "-b", "2", "-b", "3",
	sourcePath, tmpPath); err != nil {
	return out, fmt.Errorf("gdal_translate for stats: %w", err)
    }

    cmd := exec.Command("gdalinfo", "-json", "-approx_stats", tmpPath)
    raw, err := cmd.Output()
    if err != nil {
	if ee, ok := err.(*exec.ExitError); ok {
	    return out, fmt.Errorf("gdalinfo: %s", strings.TrimSpace(string(ee.Stderr)))
	}
	return out, err
    }

    return parseStatsJSON(raw)
}

func parseStatsJSON(raw []byte) ([3]bandStats, error) {
    var out [3]bandStats
    dec := json.NewDecoder(bytes.NewReader(raw))
    dec.UseNumber()
    var payload map[string]any
    if err := dec.Decode(&payload); err != nil {
	return out, fmt.Errorf("decode gdalinfo json: %w", err)
    }
    bandsRaw, ok := payload["bands"].([]any)
    if !ok || len(bandsRaw) < 3 {
	return out, errors.New("gdalinfo: need 3 bands")
    }
    for i := 0; i < 3; i++ {
	bm, ok := bandsRaw[i].(map[string]any)
	if !ok {
	    return out, fmt.Errorf("band %d: unexpected format", i)
	}
	minv, e1 := extractNumber(bm, "minimum", "STATISTICS_MINIMUM")
	maxv, e2 := extractNumber(bm, "maximum", "STATISTICS_MAXIMUM")
	meanv, e3 := extractNumber(bm, "mean", "STATISTICS_MEAN")
	stdv, e4 := extractNumber(bm, "stdDev", "STATISTICS_STDDEV")
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
	    if md, ok := bm["metadata"].(map[string]any); ok {
		if sec, ok := md[""].(map[string]any); ok {
		    minv, _ = extractNumber(sec, "STATISTICS_MINIMUM")
		    maxv, _ = extractNumber(sec, "STATISTICS_MAXIMUM")
		    meanv, _ = extractNumber(sec, "STATISTICS_MEAN")
		    stdv, _ = extractNumber(sec, "STATISTICS_STDDEV")
		}
	    }
	}
	if maxv <= minv {
	    maxv = minv + 1
	}
	out[i] = bandStats{Min: minv, Max: maxv, Mean: meanv, StdDev: stdv}
    }
    return out, nil
}

func extractNumber(m map[string]any, keys ...string) (float64, error) {
    for _, k := range keys {
	v, ok := m[k]
	if !ok {
	    continue
	}
	switch x := v.(type) {
	case float64:
	    return x, nil
	case json.Number:
	    return x.Float64()
	case string:
	    return strconv.ParseFloat(x, 64)
	}
    }
    return 0, fmt.Errorf("not found: %v", keys)
}

// computeGlobalScale вычисляет единый диапазон нормализации по медиане всех тайлов.
// Ключевой шаг для бесшовного стыка: все тайлы нормализуются одинаково.
func computeGlobalScale(selections []tileSelection) [3]scaleRange {
    var out [3]scaleRange
    for band := 0; band < 3; band++ {
	var lows, highs []float64
	for _, sel := range selections {
	    st := sel.Stats[band]
	    std := math.Max(st.StdDev, 1.0)
	    low := math.Max(1.0, st.Mean-2.0*std)
	    high := math.Min(float64(65535), st.Mean+2.0*std)
	    if high <= low {
		low = math.Max(1.0, st.Min)
		high = math.Max(low+1.0, st.Max)
	    }
	    lows = append(lows, low)
	    highs = append(highs, high)
	}
	sort.Float64s(lows)
	sort.Float64s(highs)
	srcMin := median(lows)
	srcMax := median(highs)
	if srcMax <= srcMin {
	    srcMin, srcMax = 1, 10000
	}
	out[band] = scaleRange{SrcMin: srcMin, SrcMax: srcMax, DstMin: 1, DstMax: 255}
    }
    return out
}

func median(v []float64) float64 {
    if len(v) == 0 {
	return 0
    }
    n := len(v)
    if n%2 == 1 {
	return v[n/2]
    }
    return (v[n/2-1] + v[n/2]) / 2.0
}

// normalizeTile применяет единую гамму нормализации → Byte GTiff.
func normalizeTile(workDir string, sel *tileSelection, scale [3]scaleRange) error {
    dst := filepath.Join(workDir, sel.TileID+"_norm.tif")
    args := []string{
	"-q",
	"-of", "GTiff",
	"-ot", "Byte",
	"-b", "1", "-b", "2", "-b", "3",
	"-a_nodata", "0",
	"-co", "TILED=YES",
	"-co", "COMPRESS=DEFLATE",
	"-co", "PREDICTOR=2",
	"-co", "ZLEVEL=1",
	"-co", "BLOCKXSIZE=512",
	"-co", "BLOCKYSIZE=512",
    }
    for i, r := range scale {
	args = append(args,
	    "-scale_"+strconv.Itoa(i+1),
	    ff(r.SrcMin), ff(r.SrcMax),
	    ff(r.DstMin), ff(r.DstMax),
	)
    }
    args = append(args, sel.SourcePath, dst)
    log.Printf("normalize tile=%s -> %s", sel.TileID, dst)
    if err := runCmd("gdal_translate", args...); err != nil {
	return fmt.Errorf("normalize tile=%s: %w", sel.TileID, err)
    }
    sel.NormTIF = dst
    return nil
}

// warpTile репроецирует нормализованный тайл в целевую систему координат.
func warpTile(workDir string, sel *tileSelection, targetSRS string, pixelSize float64, warpMemMB int) error {
    if warpMemMB <= 0 {
	warpMemMB = 512
    }
    dst := filepath.Join(workDir, sel.TileID+"_warp.tif")
    args := []string{
	"-q", "-overwrite",
	"-r", "bilinear",
	"-wm", strconv.Itoa(warpMemMB),
	"-wo", "NUM_THREADS=ALL_CPUS",
	"-t_srs", targetSRS,
	"-tr", ff(pixelSize), ff(pixelSize),
	"-tap",
	"-srcnodata", "0 0 0",
	"-dstnodata", "0 0 0",
	"-dstalpha",
	"-co", "TILED=YES",
	"-co", "COMPRESS=DEFLATE",
	"-co", "PREDICTOR=2",
	"-co", "ZLEVEL=1",
	"-co", "BLOCKXSIZE=512",
	"-co", "BLOCKYSIZE=512",
	sel.NormTIF, dst,
    }
    log.Printf("warp tile=%s -> %s", sel.TileID, dst)
    if err := runCmd("gdalwarp", args...); err != nil {
	return fmt.Errorf("warp tile=%s: %w", sel.TileID, err)
    }
    sel.WarpedTIF = dst
    return nil
}

// buildMosaic собирает все тайлы в финальный JP2 через VRT.
func buildMosaic(workDir string, selections []tileSelection, outPath string) error {
    listFile := filepath.Join(workDir, "sources.txt")
    f, err := os.Create(listFile)
    if err != nil {
	return fmt.Errorf("create sources list: %w", err)
    }
    w := bufio.NewWriter(f)
    for _, sel := range selections {
	if sel.WarpedTIF == "" {
	    _ = f.Close()
	    return fmt.Errorf("tile %s: no warped file", sel.TileID)
	}
	fmt.Fprintln(w, sel.WarpedTIF)
    }
    if err := w.Flush(); err != nil {
	_ = f.Close()
	return err
    }
    _ = f.Close()

    mosaicVRT := filepath.Join(workDir, "mosaic.vrt")
    if err := runCmd("gdalbuildvrt",
	"-q", "-overwrite",
	"-input_file_list", listFile,
	mosaicVRT); err != nil {
	return err
    }

    log.Printf("building JP2 mosaic -> %s", outPath)
    return runCmd("gdal_translate",
	"-q",
	"-of", "JP2OpenJPEG",
	"-b", "1", "-b", "2", "-b", "3",
	"-co", "QUALITY=15",
	"-co", "REVERSIBLE=NO",
	"-co", "YCBCR420=YES",
	"-co", "BLOCKXSIZE=1024",
	"-co", "BLOCKYSIZE=1024",
	"-co", "PROGRESSION=LRCP",
	"-co", "RESOLUTIONS=6",
	mosaicVRT, outPath,
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Вспомогательные функции
// ─────────────────────────────────────────────────────────────────────────────

func loadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
	return nil, fmt.Errorf("read config: %w", err)
    }
    var cfg Config
    if err := json.Unmarshal(data, &cfg); err != nil {
	return nil, fmt.Errorf("parse config: %w", err)
    }
    for field, val := range map[string]string{
	"results_path": cfg.ResultsPath,
	"logs":         cfg.Logs,
	"tmp":          cfg.Tmp,
    } {
	if strings.TrimSpace(val) == "" {
	    return nil, fmt.Errorf("config field '%s' is empty", field)
	}
    }
    if strings.TrimSpace(cfg.Sentinel2A) == "" && strings.TrimSpace(cfg.Sentinel) == "" {
	return nil, fmt.Errorf("config: neither sentinel2A nor sentinel path is set")
    }
    return &cfg, nil
}

func readCache(path string) (*TileCache, error) {
    data, err := os.ReadFile(path)
    if err != nil {
	return nil, fmt.Errorf("read cash.json %s: %w", path, err)
    }
    var cache TileCache
    if err := json.Unmarshal(data, &cache); err != nil {
	return nil, fmt.Errorf("parse cash.json %s: %w", path, err)
    }
    return &cache, nil
}

func parseCapturedAt(entry SafeTileEntry) (time.Time, bool) {
    for _, s := range []string{entry.CapturedAt, entry.Date} {
	s = strings.TrimSpace(s)
	if s == "" {
	    continue
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02"} {
	    if t, err := time.Parse(layout, s); err == nil {
		return t, true
	    }
	}
    }
    return time.Time{}, false
}

func normalizeTileID(raw string) string {
    return strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(raw)), "T")
}

func requireExecutables(names ...string) error {
    var missing []string
    for _, n := range names {
	if _, err := exec.LookPath(n); err != nil {
	    missing = append(missing, n)
	}
    }
    if len(missing) > 0 {
	return fmt.Errorf("GDAL tools not found: %s", strings.Join(missing, ", "))
    }
    return nil
}

func setGDALEnv(cacheMB, threads int) {
    if cacheMB <= 0 {
	cacheMB = 512
    }
    if threads <= 0 {
	threads = 4
    }
    _ = os.Setenv("GDAL_CACHEMAX", strconv.Itoa(cacheMB))
    _ = os.Setenv("GDAL_NUM_THREADS", strconv.Itoa(threads))
    _ = os.Setenv("VSI_CACHE", "FALSE")
    _ = os.Setenv("OMP_NUM_THREADS", strconv.Itoa(threads))
    _ = os.Setenv("OPENBLAS_NUM_THREADS", "1")
    _ = os.Setenv("MKL_NUM_THREADS", "1")
}

func runCmd(name string, args ...string) error {
    log.Printf("exec: %s %s", name, strings.Join(args, " "))
    cmd := exec.Command(name, args...)
    out, err := cmd.CombinedOutput()
    if len(out) > 0 {
	log.Printf("  output: %s", strings.TrimSpace(string(out)))
    }
    if err != nil {
	return fmt.Errorf("%s: %w", name, err)
    }
    return nil
}

func ff(v float64) string {
    return strconv.FormatFloat(v, 'f', 4, 64)
}