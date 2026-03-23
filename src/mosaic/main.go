package main

import (
    "bufio"
    "bytes"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "log"
    "math"
    "os"
    "os/exec"
    "path/filepath"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "time"
)

type Config struct {
    Sentinel    string `json:"sentinel"`
    ResultsPath string `json:"results_path"`
    Logs        string `json:"logs"`
    Tmp         string `json:"tmp"`
}

type TileCache struct {
    Version    int             `json:"version"`
    Preprocess string          `json:"preprocess"`
    Generated  string          `json:"generated"`
    Year       int             `json:"year"`
    Tiles      []SafeTileEntry `json:"tiles"`
}

type SafeTileEntry struct {
    Date        string     `json:"date"`
    CapturedAt  string     `json:"captured_at,omitempty"`
    SceneID     string     `json:"scene_id,omitempty"`
    SafeName    string     `json:"safe_name,omitempty"`
    ImgDataPath string     `json:"img_data_path"`
    Cloud       float64    `json:"cloud"`
    Envelope    [4]float64 `json:"envelope"`
    TileID      string     `json:"tile_id"`
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
    TileID      string
    Entry       SafeTileEntry
    CapturedAt  time.Time
    Threshold   int
    Cloud       float64
    SourcePath  string
    SourceKind  string // TCI or RGB
    RGBVRT      string
    NormTIF     string
    WarpedTIF   string
    Stats       [3]bandStats
    SourceReady bool
}

var tiles = []string{
    "39VWJ", "39VXJ", "40VCP", "40VDP", "40VEP", "40VFP",
    "39VWH", "39VXH", "40VCN", "40VDN", "40VEN", "40VFN",
    "39VWG", "39VXG", "40VCM", "40VDM", "40VEM", "40VFM",
    "39VWF", "39VXF", "40VCL", "40VDL", "40VEL", "40VFL",
    "39VWE", "39VXE", "40VCK", "40VDK", "40VEK", "40VFK",
    "39VWD", "39VXD", "40VCJ", "40VDJ", "40VEJ", "40VFJ",
    "39VWC", "39VXC", "40VCH", "40VDH", "40VEH", "40VFH",
}

var bandPatterns = map[string][]string{
    "B04": {`(?i)_B04(?:_10m)?\.jp2$`, `(?i)B04\.jp2$`},
    "B03": {`(?i)_B03(?:_10m)?\.jp2$`, `(?i)B03\.jp2$`},
    "B02": {`(?i)_B02(?:_10m)?\.jp2$`, `(?i)B02\.jp2$`},
}

var tciPatterns = []string{
    `(?i)_TCI(?:_10m)?\.jp2$`,
    `(?i)TCI\.jp2$`,
}

func main() {
    configPath := flag.String("config", filepath.Join("data", "config.json"), "path to config.json")
    startArg := flag.String("start", "", "start date in format YYYY-MM-DD")
    endArg := flag.String("end", "", "end date in format YYYY-MM-DD")
    outName := flag.String("out", "", "output file name without extension")
    keepTmp := flag.Bool("keep-tmp", false, "keep temporary working directory")
    targetSRS := flag.String("t-srs", "EPSG:3857", "target projection for warped mosaic")
    pixelSize := flag.Float64("tr", 10.0, "target pixel size in map units")
    gdalCacheMB := flag.Int("gdal-cache-mb", 256, "GDAL cache size in MB")
    warpMemMB := flag.Int("warp-mem-mb", 256, "gdalwarp working memory in MB")
    threads := flag.Int("threads", 1, "number of threads for GDAL/OMP")
    flag.Parse()

    err := run(
	*configPath,
	*startArg,
	*endArg,
	*outName,
	*keepTmp,
	*targetSRS,
	*pixelSize,
	*gdalCacheMB,
	*warpMemMB,
	*threads,
    )
    if err != nil {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
    }
}

func run(
    configPath, startArg, endArg, outName string,
    keepTmp bool,
    targetSRS string,
    pixelSize float64,
    gdalCacheMB, warpMemMB, threads int,
) error {
    if strings.TrimSpace(startArg) == "" || strings.TrimSpace(endArg) == "" {
	return errors.New("both --start and --end are required")
    }
    if strings.TrimSpace(outName) == "" {
	return errors.New("--out is required")
    }
    if pixelSize <= 0 {
	return errors.New("--tr must be > 0")
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
	return errors.New("--end must be greater than or equal to --start")
    }

    cfg, err := loadConfig(configPath)
    if err != nil {
	return err
    }

    if err := os.MkdirAll(cfg.Logs, 0o755); err != nil {
	return fmt.Errorf("create logs dir: %w", err)
    }
    logFile, err := os.OpenFile(filepath.Join(cfg.Logs, "mosaic.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
    if err != nil {
	return fmt.Errorf("open mosaic.log: %w", err)
    }
    defer logFile.Close()

    log.SetOutput(logFile)
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)

    setProcessEnvLimits(gdalCacheMB, threads)

    log.Printf("mosaic started config=%s start=%s end=%s out=%s t_srs=%s tr=%.3f",
	configPath, start.Format("2006-01-02"), end.Format("2006-01-02"), outName, targetSRS, pixelSize)
    log.Printf("config sentinel=%s results_path=%s logs=%s tmp=%s",
	cfg.Sentinel, cfg.ResultsPath, cfg.Logs, cfg.Tmp)
    log.Printf("resource limits: GDAL_CACHEMAX=%dMB warpMem=%dMB threads=%d",
	gdalCacheMB, warpMemMB, threads)

    if err := requireExecutables("gdalinfo", "gdalbuildvrt", "gdal_translate", "gdalwarp"); err != nil {
	return err
    }
    if err := os.MkdirAll(cfg.ResultsPath, 0o755); err != nil {
	return fmt.Errorf("create results_path: %w", err)
    }
    if err := os.MkdirAll(cfg.Tmp, 0o755); err != nil {
	return fmt.Errorf("create tmp dir: %w", err)
    }

    selections, err := selectBestScenesByCloud(cfg.Sentinel, start, end)
    if err != nil {
	return err
    }
    log.Printf("selected %d tile scenes", len(selections))

    workDir, err := os.MkdirTemp(cfg.Tmp, "mosaic-*")
    if err != nil {
	return fmt.Errorf("create temp dir in %s: %w", cfg.Tmp, err)
    }
    if !keepTmp {
	defer os.RemoveAll(workDir)
    }
    log.Printf("working directory=%s", workDir)

    for i := range selections {
	if err := prepareTileInputs(workDir, &selections[i]); err != nil {
	    return err
	}
    }

    globalScale := computeGlobalScale(selections)
    log.Printf("global scale R=[%.3f..%.3f] G=[%.3f..%.3f] B=[%.3f..%.3f]",
	globalScale[0].SrcMin, globalScale[0].SrcMax,
	globalScale[1].SrcMin, globalScale[1].SrcMax,
	globalScale[2].SrcMin, globalScale[2].SrcMax,
    )

    for i := range selections {
	if err := normalizeTileWithGlobalScale(workDir, &selections[i], globalScale); err != nil {
	    return err
	}
	if err := warpTileToCommonGrid(workDir, &selections[i], targetSRS, pixelSize, warpMemMB); err != nil {
	    return err
	}

	if !keepTmp {
	    if selections[i].NormTIF != "" {
		_ = os.Remove(selections[i].NormTIF)
		selections[i].NormTIF = ""
	    }
	    if selections[i].RGBVRT != "" {
		_ = os.Remove(selections[i].RGBVRT)
		selections[i].RGBVRT = ""
	    }
	}
    }

    outPath := filepath.Join(cfg.ResultsPath, outName+".jp2")
    if err := buildMosaic(workDir, selections, outPath); err != nil {
	return err
    }

    log.Printf("mosaic finished successfully: %s", outPath)
    fmt.Println(outPath)
    return nil
}

func loadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
	return nil, fmt.Errorf("read config: %w", err)
    }
    var cfg Config
    if err := json.Unmarshal(data, &cfg); err != nil {
	return nil, fmt.Errorf("parse config: %w", err)
    }
    if strings.TrimSpace(cfg.Sentinel) == "" {
	return nil, errors.New("config field 'sentinel' is empty")
    }
    if strings.TrimSpace(cfg.ResultsPath) == "" {
	return nil, errors.New("config field 'results_path' is empty")
    }
    if strings.TrimSpace(cfg.Logs) == "" {
	return nil, errors.New("config field 'logs' is empty")
    }
    if strings.TrimSpace(cfg.Tmp) == "" {
	return nil, errors.New("config field 'tmp' is empty")
    }
    return &cfg, nil
}

func requireExecutables(names ...string) error {
    var missing []string
    for _, name := range names {
	if _, err := exec.LookPath(name); err != nil {
	    missing = append(missing, name)
	}
    }
    if len(missing) > 0 {
	msg := fmt.Sprintf("required GDAL tools are not found in PATH: %s", strings.Join(missing, ", "))
	log.Print(msg)
	return errors.New(msg)
    }
    return nil
}

func setProcessEnvLimits(gdalCacheMB, threads int) {
    if gdalCacheMB <= 0 {
	gdalCacheMB = 256
    }
    if threads <= 0 {
	threads = 1
    }
    _ = os.Setenv("GDAL_CACHEMAX", strconv.Itoa(gdalCacheMB))
    _ = os.Setenv("VSI_CACHE", "FALSE")
    _ = os.Setenv("GDAL_NUM_THREADS", strconv.Itoa(threads))
    _ = os.Setenv("OMP_NUM_THREADS", strconv.Itoa(threads))
    _ = os.Setenv("OPENBLAS_NUM_THREADS", "1")
    _ = os.Setenv("MKL_NUM_THREADS", "1")
}

func selectBestScenesByCloud(sentinelRoot string, start, end time.Time) ([]tileSelection, error) {
    tileSet := make(map[string]struct{}, len(tiles))
    candidatesByTile := make(map[string][]tileCandidate, len(tiles))
    endInclusive := end.Add(23*time.Hour + 59*time.Minute + 59*time.Second)

    for _, t := range tiles {
	tileSet[t] = struct{}{}
    }

    for year := start.Year(); year <= end.Year(); year++ {
	cachePath := filepath.Join(sentinelRoot, strconv.Itoa(year), "cash.json")
	cache, err := readCache(cachePath)
	if err != nil {
	    return nil, err
	}
	log.Printf("read cache=%s entries=%d", cachePath, len(cache.Tiles))

	for _, entry := range cache.Tiles {
	    tileID := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(entry.TileID)), "T")
	    if _, ok := tileSet[tileID]; !ok {
		continue
	    }
	    capturedAt, ok := parseCapturedAt(entry)
	    if !ok {
		log.Printf("skip invalid date tile=%s safe=%s date=%s captured_at=%s",
		    tileID, entry.SafeName, entry.Date, entry.CapturedAt)
		continue
	    }
	    if capturedAt.Before(start) || capturedAt.After(endInclusive) {
		continue
	    }
	    candidatesByTile[tileID] = append(candidatesByTile[tileID], tileCandidate{
		TileID:     tileID,
		Entry:      entry,
		CapturedAt: capturedAt,
	    })
	}
    }

    var out []tileSelection
    var missing []string
    for _, tileID := range tiles {
	candidates := candidatesByTile[tileID]
	if len(candidates) == 0 {
	    missing = append(missing, tileID)
	    continue
	}

	selected, threshold, ok := pickCandidateByThreshold(candidates)
	if !ok {
	    missing = append(missing, tileID)
	    continue
	}

	log.Printf("selected tile=%s threshold<=%d cloud=%.6f captured_at=%s safe=%s img_data=%s",
	    tileID, threshold, selected.Entry.Cloud, selected.CapturedAt.Format(time.RFC3339),
	    selected.Entry.SafeName, selected.Entry.ImgDataPath)

	out = append(out, tileSelection{
	    TileID:     tileID,
	    Entry:      selected.Entry,
	    CapturedAt: selected.CapturedAt,
	    Threshold:  threshold,
	    Cloud:      selected.Entry.Cloud,
	})
    }

    if len(missing) > 0 {
	return nil, fmt.Errorf("no scenes found in period %s..%s for tiles: %s",
	    start.Format("2006-01-02"), end.Format("2006-01-02"), strings.Join(missing, ", "))
    }

    return out, nil
}

func pickCandidateByThreshold(candidates []tileCandidate) (tileCandidate, int, bool) {
    if len(candidates) == 0 {
	return tileCandidate{}, 0, false
    }

    maxCloud := 1
    for _, c := range candidates {
	v := int(math.Ceil(c.Entry.Cloud))
	if v > maxCloud {
	    maxCloud = v
	}
    }

    for threshold := 1; threshold <= maxCloud; threshold++ {
	var best tileCandidate
	found := false

	for _, c := range candidates {
	    if c.Entry.Cloud > float64(threshold) {
		continue
	    }
	    if !found || c.CapturedAt.After(best.CapturedAt) {
		best = c
		found = true
	    }
	}

	if found {
	    return best, threshold, true
	}
    }

    return tileCandidate{}, 0, false
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
    if s := strings.TrimSpace(entry.CapturedAt); s != "" {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
	    return t, true
	}
	if t, err := time.Parse("2006-01-02 15:04:05", s); err == nil {
	    return t, true
	}
    }
    if s := strings.TrimSpace(entry.Date); s != "" {
	if t, err := time.Parse("2006-01-02", s); err == nil {
	    return t, true
	}
    }
    return time.Time{}, false
}

func prepareTileInputs(workDir string, sel *tileSelection) error {
    tci, err := findTCIFile(sel.Entry.ImgDataPath)
    if err == nil && strings.TrimSpace(tci) != "" {
	sel.SourcePath = tci
	sel.SourceKind = "TCI"
	stats, err := collectStats(tci)
	if err != nil {
	    return fmt.Errorf("tile %s: collect TCI stats: %w", sel.TileID, err)
	}
	sel.Stats = stats
	sel.SourceReady = true
	log.Printf("source tile=%s kind=TCI file=%s cloud=%.6f thr<=%d",
	    sel.TileID, tci, sel.Cloud, sel.Threshold)
	return nil
    }

    red, err := findBandFile(sel.Entry.ImgDataPath, "B04")
    if err != nil {
	return fmt.Errorf("tile %s: neither TCI nor B04 found: %w", sel.TileID, err)
    }
    green, err := findBandFile(sel.Entry.ImgDataPath, "B03")
    if err != nil {
	return fmt.Errorf("tile %s: fallback B03 not found: %w", sel.TileID, err)
    }
    blue, err := findBandFile(sel.Entry.ImgDataPath, "B02")
    if err != nil {
	return fmt.Errorf("tile %s: fallback B02 not found: %w", sel.TileID, err)
    }

    rgbVRT := filepath.Join(workDir, fmt.Sprintf("%s_rgb.vrt", sel.TileID))
    if err := buildRGBVRT(rgbVRT, red, green, blue); err != nil {
	return fmt.Errorf("tile %s: build RGB VRT: %w", sel.TileID, err)
    }

    sel.SourcePath = rgbVRT
    sel.SourceKind = "RGB"
    sel.RGBVRT = rgbVRT

    stats, err := collectStats(rgbVRT)
    if err != nil {
	return fmt.Errorf("tile %s: collect RGB stats: %w", sel.TileID, err)
    }
    sel.Stats = stats
    sel.SourceReady = true

    log.Printf("source tile=%s kind=RGB file=%s cloud=%.6f thr<=%d",
	sel.TileID, rgbVRT, sel.Cloud, sel.Threshold)

    return nil
}

func findTCIFile(imgDataPath string) (string, error) {
    entries, err := os.ReadDir(imgDataPath)
    if err != nil {
	return "", fmt.Errorf("read IMG_DATA %s: %w", imgDataPath, err)
    }

    var regexps []*regexp.Regexp
    for _, p := range tciPatterns {
	regexps = append(regexps, regexp.MustCompile(p))
    }

    var candidates []string
    for _, entry := range entries {
	if entry.IsDir() {
	    continue
	}
	name := entry.Name()
	for _, re := range regexps {
	    if re.MatchString(name) {
		candidates = append(candidates, filepath.Join(imgDataPath, name))
		break
	    }
	}
    }

    sort.Strings(candidates)
    if len(candidates) == 0 {
	return "", errors.New("TCI not found")
    }
    return candidates[0], nil
}

func findBandFile(imgDataPath, band string) (string, error) {
    entries, err := os.ReadDir(imgDataPath)
    if err != nil {
	return "", fmt.Errorf("read IMG_DATA %s: %w", imgDataPath, err)
    }

    patterns := bandPatterns[band]
    var regexps []*regexp.Regexp
    for _, p := range patterns {
	regexps = append(regexps, regexp.MustCompile(p))
    }

    var candidates []string
    for _, entry := range entries {
	if entry.IsDir() {
	    continue
	}
	name := entry.Name()
	for _, re := range regexps {
	    if re.MatchString(name) {
		candidates = append(candidates, filepath.Join(imgDataPath, name))
		break
	    }
	}
    }

    sort.Strings(candidates)
    if len(candidates) == 0 {
	return "", fmt.Errorf("band %s not found in %s", band, imgDataPath)
    }
    return candidates[0], nil
}

func buildRGBVRT(path, red, green, blue string) error {
    return runCmd("gdalbuildvrt", "-q", "-separate", "-srcnodata", "0", "-vrtnodata", "0", path, red, green, blue)
}

func collectStats(path string) ([3]bandStats, error) {
    var out [3]bandStats

    cmd := exec.Command("gdalinfo", "-json", "-stats", path)
    raw, err := cmd.Output()
    if err != nil {
	if ee, ok := err.(*exec.ExitError); ok {
	    return out, fmt.Errorf("gdalinfo failed: %s", strings.TrimSpace(string(ee.Stderr)))
	}
	return out, err
    }

    decoder := json.NewDecoder(bytes.NewReader(raw))
    decoder.UseNumber()

    var payload map[string]any
    if err := decoder.Decode(&payload); err != nil {
	return out, fmt.Errorf("decode gdalinfo json: %w", err)
    }

    bandsRaw, ok := payload["bands"].([]any)
    if !ok || len(bandsRaw) < 3 {
	return out, errors.New("gdalinfo json does not contain 3 bands")
    }

    for i := 0; i < 3; i++ {
	bandMap, ok := bandsRaw[i].(map[string]any)
	if !ok {
	    return out, errors.New("unexpected band format in gdalinfo json")
	}

	statsMap, ok := bandMap["metadata"].(map[string]any)
	if !ok {
	    return out, errors.New("missing metadata in gdalinfo json")
	}

	statSection, ok := statsMap[""].(map[string]any)
	if !ok {
	    statSection = statsMap
	}

	minv, err := findFloat(statSection, "STATISTICS_MINIMUM", "minimum")
	if err != nil {
	    return out, err
	}
	maxv, err := findFloat(statSection, "STATISTICS_MAXIMUM", "maximum")
	if err != nil {
	    return out, err
	}
	meanv, err := findFloat(statSection, "STATISTICS_MEAN", "mean")
	if err != nil {
	    return out, err
	}
	stdv, err := findFloat(statSection, "STATISTICS_STDDEV", "stdDev")
	if err != nil {
	    return out, err
	}

	out[i] = bandStats{
	    Min:    minv,
	    Max:    maxv,
	    Mean:   meanv,
	    StdDev: stdv,
	}
    }

    return out, nil
}

func findFloat(m map[string]any, keys ...string) (float64, error) {
    for _, key := range keys {
	v, ok := m[key]
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
    return 0, fmt.Errorf("no numeric field found among keys %v", keys)
}

func computeGlobalScale(selections []tileSelection) [3]scaleRange {
    var out [3]scaleRange

    for band := 0; band < 3; band++ {
	var lows []float64
	var highs []float64

	for _, sel := range selections {
	    st := sel.Stats[band]
	    std := math.Max(st.StdDev, 1.0)

	    low := math.Max(1.0, st.Mean-2.0*std)
	    high := math.Min(st.Max, st.Mean+2.0*std)

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
	    srcMin = 1
	    srcMax = 255
	}

	out[band] = scaleRange{
	    SrcMin: srcMin,
	    SrcMax: srcMax,
	    DstMin: 1,
	    DstMax: 255,
	}
    }

    return out
}

func median(values []float64) float64 {
    if len(values) == 0 {
	return 0
    }
    n := len(values)
    if n%2 == 1 {
	return values[n/2]
    }
    return (values[n/2-1] + values[n/2]) / 2.0
}

func normalizeTileWithGlobalScale(workDir string, sel *tileSelection, scale [3]scaleRange) error {
    dst := filepath.Join(workDir, fmt.Sprintf("%s_norm.tif", sel.TileID))

    args := []string{
	"-q",
	"-of", "GTiff",
	"-ot", "Byte",
	"-b", "1",
	"-b", "2",
	"-b", "3",
	"-a_nodata", "0",
	"-co", "TILED=YES",
	"-co", "COMPRESS=DEFLATE",
	"-co", "PREDICTOR=2",
	"-co", "ZLEVEL=4",
	"-co", "BLOCKXSIZE=512",
	"-co", "BLOCKYSIZE=512",
    }

    for band := 0; band < 3; band++ {
	r := scale[band]
	args = append(args,
	    "-scale_"+strconv.Itoa(band+1),
	    formatFloat(r.SrcMin),
	    formatFloat(r.SrcMax),
	    formatFloat(r.DstMin),
	    formatFloat(r.DstMax),
	)
    }

    args = append(args, sel.SourcePath, dst)

    log.Printf("normalize tile=%s kind=%s src=%s dst=%s cloud=%.6f",
	sel.TileID, sel.SourceKind, sel.SourcePath, dst, sel.Cloud)

    if err := runCmd("gdal_translate", args...); err != nil {
	return err
    }

    sel.NormTIF = dst
    return nil
}

func warpTileToCommonGrid(workDir string, sel *tileSelection, targetSRS string, pixelSize float64, warpMemMB int) error {
    if warpMemMB <= 0 {
	warpMemMB = 256
    }

    dst := filepath.Join(workDir, fmt.Sprintf("%s_warp.tif", sel.TileID))
    args := []string{
	"-q",
	"-overwrite",
	"-r", "bilinear",
	"-wm", strconv.Itoa(warpMemMB),
	"-wo", "NUM_THREADS=1",
	"-wo", "OPTIMIZE_SIZE=YES",
	"-t_srs", targetSRS,
	"-tr", formatFloat(pixelSize), formatFloat(pixelSize),
	"-tap",
	"-srcnodata", "0 0 0",
	"-dstnodata", "0 0 0",
	"-dstalpha",
	"-co", "TILED=YES",
	"-co", "COMPRESS=DEFLATE",
	"-co", "PREDICTOR=2",
	"-co", "ZLEVEL=4",
	"-co", "BLOCKXSIZE=512",
	"-co", "BLOCKYSIZE=512",
	sel.NormTIF,
	dst,
    }

    log.Printf("warp tile=%s dst=%s warpMemMB=%d", sel.TileID, dst, warpMemMB)
    if err := runCmd("gdalwarp", args...); err != nil {
	return err
    }

    sel.WarpedTIF = dst
    return nil
}

func buildMosaic(workDir string, selections []tileSelection, outPath string) error {
    vrtList := filepath.Join(workDir, "mosaic_sources.txt")
    f, err := os.Create(vrtList)
    if err != nil {
	return fmt.Errorf("create source list: %w", err)
    }

    w := bufio.NewWriter(f)
    for _, sel := range selections {
	if strings.TrimSpace(sel.WarpedTIF) == "" {
	    _ = f.Close()
	    return fmt.Errorf("tile %s: warped file is empty", sel.TileID)
	}
	if _, err := fmt.Fprintln(w, sel.WarpedTIF); err != nil {
	    _ = f.Close()
	    return fmt.Errorf("write source list: %w", err)
	}
    }
    if err := w.Flush(); err != nil {
	_ = f.Close()
	return fmt.Errorf("flush source list: %w", err)
    }
    if err := f.Close(); err != nil {
	return fmt.Errorf("close source list: %w", err)
    }

    mosaicVRT := filepath.Join(workDir, "mosaic.vrt")
    if err := runCmd(
	"gdalbuildvrt",
	"-q",
	"-overwrite",
	"-input_file_list", vrtList,
	mosaicVRT,
    ); err != nil {
	return err
    }

    return runCmd(
	"gdal_translate",
	"-q",
	"-of", "JP2OpenJPEG",
	"-b", "1",
	"-b", "2",
	"-b", "3",
	"-co", "QUALITY=25",
	"-co", "REVERSIBLE=NO",
	"-co", "YCBCR420=NO",
	"-co", "BLOCKXSIZE=1024",
	"-co", "BLOCKYSIZE=1024",
	mosaicVRT,
	outPath,
    )
}

func runCmd(name string, args ...string) error {
    log.Printf("exec: %s %s", name, strings.Join(args, " "))

    cmd := exec.Command(name, args...)
    out, err := cmd.CombinedOutput()
    if len(out) > 0 {
	log.Printf("%s output:\n%s", name, string(out))
    }
    if err != nil {
	return fmt.Errorf("%s failed: %w", name, err)
    }
    return nil
}

func formatFloat(v float64) string {
    return strconv.FormatFloat(v, 'f', 6, 64)
}
