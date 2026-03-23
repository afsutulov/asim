package main

import (
    "context"
    "encoding/json"
    "errors"
    "flag"
    "fmt"
    "io"
    "log"
    "os"
    "path/filepath"
    "regexp"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "cloud.google.com/go/storage"
    "google.golang.org/api/iterator"
    "google.golang.org/api/option"
)

const bucketName = "gcp-public-data-sentinel-2"

var tiles = []string{
    "39VWJ", "39VXJ", "40VCP", "40VDP", "40VEP", "40VFP",
    "39VWH", "39VXH", "40VCN", "40VDN", "40VEN", "40VFN",
    "39VWG", "39VXG", "40VCM", "40VDM", "40VEM", "40VFM",
    "39VWF", "39VXF", "40VCL", "40VDL", "40VEL", "40VFL",
    "39VWE", "39VXE", "40VCK", "40VDK", "40VEK", "40VFK",
    "39VWD", "39VXD", "40VCJ", "40VDJ", "40VEJ", "40VFJ",
    "39VWC", "39VXC", "40VCH", "40VDH", "40VEH", "40VFH",
}

// L1C product folders, example:
// S2A_MSIL1C_20151221T205519_N0201_R028_T01CCV_20160329T181515.SAFE
var safeFolderRe = regexp.MustCompile(`^S2[AB]_MSIL1C_(\d{8})T\d{6}_.+\.SAFE$`)

type Config struct {
    Logs     string `json:"logs"`
    Sentinel string `json:"sentinel"`
}

type scenePrefix struct {
    Tile         string
    TilePrefix   string
    ProductName  string
    ProductPrefix string
    Date         time.Time
}

type runStats struct {
    Matched         int64
    Skipped         int64
    ToDownload      int64
    Downloaded      int64
    FilesDownloaded int64
    BytesDownloaded int64
}

func main() {
    var (
	startStr        = flag.String("start", "", "Start date inclusive, format YYYY-MM-DD")
	endStr          = flag.String("end", "", "End date inclusive, format YYYY-MM-DD")
	configPath      = flag.String("config", "./data/config.json", "Path to config.json")
	runMode         = flag.Bool("run", false, "Use automatic period: end=today, start=end-30 days")
	listWorkers     = flag.Int("list-workers", 8, "Concurrent tile listing workers")
	downloadWorkers = flag.Int("download-workers", 8, "Concurrent file download workers")
    )
    flag.Parse()

    cfg, err := loadConfig(*configPath)
    must(err)

    logger, logFile, err := setupLogger(cfg.Logs)
    must(err)
    defer logFile.Close()

    startDate, endDate, err := resolveDateRange(*startStr, *endStr, *runMode)
    if err != nil {
	logger.Printf("ERROR: invalid date range: %v", err)
	log.Fatal(err)
    }

    if err := os.MkdirAll(cfg.Sentinel, 0o755); err != nil {
	logger.Printf("ERROR: create sentinel root dir %s: %v", cfg.Sentinel, err)
	log.Fatal(err)
    }

    logger.Printf(
	"START: scan period %s .. %s, config=%s, sentinel_root=%s, list_workers=%d, download_workers=%d",
	startDate.Format("2006-01-02"),
	endDate.Format("2006-01-02"),
	*configPath,
	cfg.Sentinel,
	*listWorkers,
	*downloadWorkers,
    )

    ctx := context.Background()
    client, err := storage.NewClient(ctx, option.WithoutAuthentication())
    if err != nil {
	logger.Printf("ERROR: create storage client: %v", err)
	log.Fatal(err)
    }
    defer client.Close()

    scenes, err := discoverMatchingScenes(ctx, client, startDate, endDate, *listWorkers)
    if err != nil {
	logger.Printf("ERROR: discover scenes: %v", err)
	log.Fatal(err)
    }

    var stats runStats
    stats.Matched = int64(len(scenes))

    toDownload := make([]scenePrefix, 0, len(scenes))
    for _, s := range scenes {
	targetDir := sceneTargetDir(cfg.Sentinel, s)
	if dirExists(targetDir) {
	    stats.Skipped++
	    continue
	}
	toDownload = append(toDownload, s)
	stats.ToDownload++
    }

    if len(toDownload) > 0 {
	if err := downloadAllScenes(ctx, client, toDownload, cfg.Sentinel, *downloadWorkers, &stats); err != nil {
	    logger.Printf("ERROR: download failed: %v", err)
	    log.Fatal(err)
	}
    }

    logger.Printf(
	"FINISH: period %s .. %s, matched=%d, downloaded=%d, skipped_existing=%d, files_downloaded=%d, bytes_downloaded=%d",
	startDate.Format("2006-01-02"),
	endDate.Format("2006-01-02"),
	stats.Matched,
	stats.Downloaded,
	stats.Skipped,
	stats.FilesDownloaded,
	stats.BytesDownloaded,
    )
}

func loadConfig(path string) (*Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
	return nil, fmt.Errorf("read config %s: %w", path, err)
    }

    var cfg Config
    if err := json.Unmarshal(data, &cfg); err != nil {
	return nil, fmt.Errorf("parse config %s: %w", path, err)
    }

    if strings.TrimSpace(cfg.Sentinel) == "" {
	return nil, fmt.Errorf("config %s: sentinel is empty", path)
    }
    if strings.TrimSpace(cfg.Logs) == "" {
	return nil, fmt.Errorf("config %s: logs is empty", path)
    }

    return &cfg, nil
}

func setupLogger(logDir string) (*log.Logger, *os.File, error) {
    if err := os.MkdirAll(logDir, 0o755); err != nil {
	return nil, nil, fmt.Errorf("create log dir %s: %w", logDir, err)
    }

    logPath := filepath.Join(logDir, "sentinel2-download.log")
    f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
    if err != nil {
	return nil, nil, fmt.Errorf("open log file %s: %w", logPath, err)
    }

    return log.New(f, "", log.Ldate|log.Ltime), f, nil
}

func resolveDateRange(startStr, endStr string, runMode bool) (time.Time, time.Time, error) {
    if runMode {
	now := time.Now().UTC()
	endDate := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	startDate := endDate.AddDate(0, 0, -30)
	return startDate, endDate, nil
    }

    switch {
    case startStr == "" && endStr == "":
	return time.Time{}, time.Time{}, errors.New("either specify --run, or specify --end, or specify both --start and --end")
    case startStr == "" && endStr != "":
	endDate, err := parseDate(endStr)
	if err != nil {
	    return time.Time{}, time.Time{}, err
	}
	startDate := endDate.AddDate(0, 0, -30)
	return startDate, endDate, nil
    case startStr != "" && endStr == "":
	return time.Time{}, time.Time{}, errors.New("--end is required when --start is specified")
    default:
	startDate, err := parseDate(startStr)
	if err != nil {
	    return time.Time{}, time.Time{}, err
	}
	endDate, err := parseDate(endStr)
	if err != nil {
	    return time.Time{}, time.Time{}, err
	}
	if endDate.Before(startDate) {
	    return time.Time{}, time.Time{}, fmt.Errorf("end date %s is before start date %s", endDate.Format("2006-01-02"), startDate.Format("2006-01-02"))
	}
	return startDate, endDate, nil
    }
}

func parseDate(s string) (time.Time, error) {
    t, err := time.Parse("2006-01-02", s)
    if err != nil {
	return time.Time{}, fmt.Errorf("invalid date %q, expected YYYY-MM-DD: %w", s, err)
    }
    return t.UTC(), nil
}

func tileToPrefix(tile string) (string, error) {
    // 40VDP -> tiles/40/V/DP/
    if len(tile) != 5 {
	return "", fmt.Errorf("invalid tile %q: expected 5 chars", tile)
    }
    utm := tile[:2]
    latBand := tile[2:3]
    grid := tile[3:5]
    return fmt.Sprintf("tiles/%s/%s/%s/", utm, latBand, grid), nil
}

func discoverMatchingScenes(ctx context.Context, client *storage.Client, startDate, endDate time.Time, workers int) ([]scenePrefix, error) {
    type result struct {
	items []scenePrefix
	err   error
    }

    tileCh := make(chan string)
    resCh := make(chan result, len(tiles))

    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
	wg.Add(1)
	go func() {
	    defer wg.Done()
	    for tile := range tileCh {
		items, err := discoverForTile(ctx, client, tile, startDate, endDate)
		resCh <- result{items: items, err: err}
	    }
	}()
    }

    go func() {
	for _, tile := range tiles {
	    tileCh <- tile
	}
	close(tileCh)
	wg.Wait()
	close(resCh)
    }()

    var all []scenePrefix
    var errs []error

    for r := range resCh {
	if r.err != nil {
	    errs = append(errs, r.err)
	    continue
	}
	all = append(all, r.items...)
    }

    if len(errs) > 0 {
	return all, joinErrors(errs)
    }
    return all, nil
}

func discoverForTile(ctx context.Context, client *storage.Client, tile string, startDate, endDate time.Time) ([]scenePrefix, error) {
    tilePrefix, err := tileToPrefix(tile)
    if err != nil {
	return nil, err
    }

    it := client.Bucket(bucketName).Objects(ctx, &storage.Query{
	Prefix:    tilePrefix,
	Delimiter: "/",
    })

    var matches []scenePrefix

    for {
	attrs, err := it.Next()
	if err == iterator.Done {
	    break
	}
	if err != nil {
	    return nil, fmt.Errorf("list prefixes for tile %s: %w", tile, err)
	}

	if attrs.Prefix == "" {
	    continue
	}

	productName := strings.TrimSuffix(strings.TrimPrefix(attrs.Prefix, tilePrefix), "/")
	if !strings.HasSuffix(productName, ".SAFE") {
	    continue
	}

	dt, ok := parseDateFromProductName(productName)
	if !ok {
	    continue
	}
	if dt.Before(startDate) || dt.After(endDate) {
	    continue
	}

	matches = append(matches, scenePrefix{
	    Tile:          tile,
	    TilePrefix:    tilePrefix,
	    ProductName:   productName,
	    ProductPrefix: attrs.Prefix,
	    Date:          dt,
	})
    }

    return matches, nil
}

func parseDateFromProductName(name string) (time.Time, bool) {
    m := safeFolderRe.FindStringSubmatch(name)
    if len(m) != 2 {
	return time.Time{}, false
    }

    t, err := time.Parse("20060102", m[1])
    if err != nil {
	return time.Time{}, false
    }
    return t.UTC(), true
}

func sceneTargetDir(sentinelRoot string, s scenePrefix) string {
    yearDir := filepath.Join(sentinelRoot, fmt.Sprintf("%04d", s.Date.Year()))
    return filepath.Join(yearDir, s.ProductName)
}

func downloadAllScenes(ctx context.Context, client *storage.Client, scenes []scenePrefix, sentinelRoot string, workers int, stats *runStats) error {
    type job struct {
	objectName string
	localPath  string
    }

    jobs := make(chan job, workers*4)
    errCh := make(chan error, workers)

    var wg sync.WaitGroup
    for i := 0; i < workers; i++ {
	wg.Add(1)
	go func() {
	    defer wg.Done()
	    for j := range jobs {
		n, downloaded, err := downloadObject(ctx, client, j.objectName, j.localPath)
		if err != nil {
		    errCh <- fmt.Errorf("download %s -> %s: %w", j.objectName, j.localPath, err)
		    continue
		}
		if downloaded {
		    atomic.AddInt64(&stats.FilesDownloaded, 1)
		    atomic.AddInt64(&stats.BytesDownloaded, n)
		}
	    }
	}()
    }

    go func() {
	wg.Wait()
	close(errCh)
    }()

    for _, s := range scenes {
	targetDir := sceneTargetDir(sentinelRoot, s)
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
	    close(jobs)
	    return fmt.Errorf("create target dir %s: %w", targetDir, err)
	}

	it := client.Bucket(bucketName).Objects(ctx, &storage.Query{
	    Prefix: s.ProductPrefix,
	})

	for {
	    attrs, err := it.Next()
	    if err == iterator.Done {
		break
	    }
	    if err != nil {
		close(jobs)
		return fmt.Errorf("list objects under %s: %w", s.ProductPrefix, err)
	    }
	    if attrs.Name == "" || strings.HasSuffix(attrs.Name, "/") {
		continue
	    }

	    rel := strings.TrimPrefix(attrs.Name, s.ProductPrefix)
	    localPath := filepath.Join(targetDir, filepath.FromSlash(rel))

	    jobs <- job{
		objectName: attrs.Name,
		localPath:  localPath,
	    }
	}

	atomic.AddInt64(&stats.Downloaded, 1)
    }

    close(jobs)

    var errs []error
    for err := range errCh {
	errs = append(errs, err)
    }
    if len(errs) > 0 {
	return joinErrors(errs)
    }

    return nil
}

func downloadObject(ctx context.Context, client *storage.Client, objectName, localPath string) (int64, bool, error) {
    if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
	return 0, false, err
    }

    if st, err := os.Stat(localPath); err == nil && st.Size() > 0 {
	return 0, false, nil
    }

    tmpPath := localPath + ".part"

    r, err := client.Bucket(bucketName).Object(objectName).NewReader(ctx)
    if err != nil {
	return 0, false, err
    }
    defer r.Close()

    f, err := os.Create(tmpPath)
    if err != nil {
	return 0, false, err
    }

    n, copyErr := io.Copy(f, r)
    closeErr := f.Close()

    if copyErr != nil {
	_ = os.Remove(tmpPath)
	return n, false, copyErr
    }
    if closeErr != nil {
	_ = os.Remove(tmpPath)
	return n, false, closeErr
    }
    if err := os.Rename(tmpPath, localPath); err != nil {
	_ = os.Remove(tmpPath)
	return n, false, err
    }

    return n, true, nil
}

func dirExists(path string) bool {
    st, err := os.Stat(path)
    if err != nil {
	return false
    }
    return st.IsDir()
}

func joinErrors(errs []error) error {
    if len(errs) == 0 {
	return nil
    }
    if len(errs) == 1 {
	return errs[0]
    }

    var sb strings.Builder
    sb.WriteString("multiple errors:")
    for _, err := range errs {
	sb.WriteString("\n - ")
	sb.WriteString(err.Error())
    }
    return errors.New(sb.String())
}

func must(err error) {
    if err != nil {
	log.Fatal(err)
    }
}
