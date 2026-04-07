package main

/*
#cgo pkg-config: gdal
#include <stdlib.h>
#include "gdal.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"

static void _gdal_init_once() {
    GDALAllRegister();
    OGRRegisterAll();
}
static void _set_traditional_gis_order(OGRSpatialReferenceH srs) {
    if (srs != NULL) {
        OSRSetAxisMappingStrategy(srs, OAMS_TRADITIONAL_GIS_ORDER);
    }
}
*/
import "C"

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

type Config struct {
	Sentinel    string `json:"sentinel"`
	Sentinel2A  string `json:"sentinel2A"`
	Logs        string `json:"logs"`
	ResultsFile string `json:"results"`
	ResultsPath string `json:"results_path"`
}

type TileCache struct {
	Version    int             `json:"version"`
	Preprocess string          `json:"preprocess"`
	Generated  string          `json:"generated"`
	Year       int             `json:"year"`
	Tiles      []SafeTileEntry `json:"tiles"`
}

type SafeTileEntry struct {
	Date                  string     `json:"date"`
	CapturedAt            string     `json:"captured_at,omitempty"`
	SceneID               string     `json:"scene_id,omitempty"`
	SafeName              string     `json:"safe_name,omitempty"`
	ImgDataPath           string     `json:"img_data_path"`
	Cloud                 float64    `json:"cloud"`
	NodataPixelPercentage *float64   `json:"nodata_pixel_percentage,omitempty"`
	Envelope              [4]float64 `json:"envelope"`
	TileID                string     `json:"tile_id"`
}

type mtdMSIL1C struct {
	XMLName             xml.Name `xml:"Level-1C_User_Product"`
	CloudCoverageLegacy *float64 `xml:"Quality_Indicators_Info>Cloud_Coverage_Assessment"`
	CloudyPixelPercent  *float64 `xml:"Quality_Indicators_Info>CLOUDY_PIXEL_PERCENTAGE"`
}

type l2AMetadata struct {
	Cloud  float64
	Nodata float64
}

func defaultConfigPath() string { return filepath.Join("data", "config.json") }

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Sentinel) == "" {
		return nil, errors.New("sentinel is empty in config.json")
	}
	if strings.TrimSpace(cfg.Logs) == "" {
		return nil, errors.New("logs is empty in config.json")
	}
	if strings.TrimSpace(cfg.ResultsFile) == "" {
		return nil, errors.New("results is empty in config.json")
	}
	if strings.TrimSpace(cfg.ResultsPath) == "" {
		return nil, errors.New("results_path is empty in config.json")
	}
	return &cfg, nil
}

func setupLogger(logsDir string) (*os.File, error) {
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return nil, err
	}
	fp := filepath.Join(logsDir, "cron.log")
	f, err := os.OpenFile(fp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	log.SetOutput(f)
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	return f, nil
}

func parseSAFEName(name string) (dateOnly string, capturedAt string, tileID string, sceneID string, err error) {
	base := strings.TrimSuffix(filepath.Base(name), ".SAFE")
	parts := strings.Split(base, "_")
	if len(parts) < 6 {
		err = fmt.Errorf("unexpected SAFE name: %s", name)
		return
	}
	ts, e := time.Parse("20060102T150405", parts[2])
	if e != nil {
		err = e
		return
	}
	dateOnly = ts.Format("2006-01-02")
	capturedAt = ts.Format(time.RFC3339)
	for _, part := range parts {
		if len(part) == 6 && strings.HasPrefix(part, "T") {
			tileID = part
			break
		}
	}
	if tileID == "" {
		tileID = parts[5]
	}
	sceneID = tileID + "|" + capturedAt
	return
}

func findIMGData(safePath string) (string, error) {
	granuleDir := filepath.Join(safePath, "GRANULE")
	entries, err := os.ReadDir(granuleDir)
	if err != nil {
		return "", err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		p := filepath.Join(granuleDir, ent.Name(), "IMG_DATA")
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			return p, nil
		}
	}
	return "", fmt.Errorf("IMG_DATA not found in %s", granuleDir)
}

func readTileCloud(metaPath string) (float64, error) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		return 0, err
	}

	var m mtdMSIL1C
	if err := xml.Unmarshal(data, &m); err != nil {
		return 0, fmt.Errorf("xml parse failed for %s: %w", metaPath, err)
	}

	if m.CloudCoverageLegacy != nil {
		return *m.CloudCoverageLegacy, nil
	}
	if m.CloudyPixelPercent != nil {
		return *m.CloudyPixelPercent, nil
	}

	return 0, fmt.Errorf("cloud coverage not found in %s", metaPath)
}

func readL2AMetadata(metaPath string) (*l2AMetadata, error) {
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	dec := xml.NewDecoder(f)
	out := &l2AMetadata{}
	var haveCloud bool
	var haveNodata bool

	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("xml parse failed for %s: %w", metaPath, err)
		}
		se, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}

		switch se.Name.Local {
		case "Cloud_Coverage_Assessment", "CLOUDY_PIXEL_PERCENTAGE":
			var s string
			if err := dec.DecodeElement(&s, &se); err != nil {
				return nil, fmt.Errorf("xml decode failed for %s: %w", metaPath, err)
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err == nil {
				out.Cloud = v
				haveCloud = true
			}
		case "NODATA_PIXEL_PERCENTAGE":
			var s string
			if err := dec.DecodeElement(&s, &se); err != nil {
				return nil, fmt.Errorf("xml decode failed for %s: %w", metaPath, err)
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
			if err == nil {
				out.Nodata = v
				haveNodata = true
			}
		}
	}

	if !haveCloud {
		return nil, fmt.Errorf("cloud coverage not found in %s", metaPath)
	}
	if !haveNodata {
		return nil, fmt.Errorf("NODATA_PIXEL_PERCENTAGE not found in %s", metaPath)
	}
	return out, nil
}

func firstJP2InDir(imgDataPath string) (string, error) {
	var found string
	err := filepath.WalkDir(imgDataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		if strings.HasSuffix(name, ".jp2") && !strings.Contains(name, "_tci.") {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("no jp2 found in %s", imgDataPath)
	}
	return found, nil
}

func findBandJP2(imgDataPath, band string) (string, error) {
	needle := "_" + strings.ToLower(strings.TrimSpace(band)) + ".jp2"
	var found string

	err := filepath.WalkDir(imgDataPath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		name := strings.ToLower(d.Name())
		if strings.HasSuffix(name, needle) {
			found = path
			return filepath.SkipAll
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	if found == "" {
		return "", fmt.Errorf("band %s not found in %s", band, imgDataPath)
	}
	return found, nil
}

func shouldIndexByB02Size(imgDataPath string, minBytes int64) (bool, error) {
	b02, err := findBandJP2(imgDataPath, "B02")
	if err != nil {
		return false, err
	}
	st, err := os.Stat(b02)
	if err != nil {
		return false, err
	}
	return st.Size() >= minBytes, nil
}

func envelopeWGS84FromJP2(path string) ([4]float64, error) {
	var env [4]float64
	C._gdal_init_once()
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	ds := C.GDALOpen(cp, C.GA_ReadOnly)
	if ds == nil {
		return env, fmt.Errorf("open raster: %s", path)
	}
	defer C.GDALClose(ds)
	w, h := int(C.GDALGetRasterXSize(ds)), int(C.GDALGetRasterYSize(ds))
	if w <= 0 || h <= 0 {
		return env, fmt.Errorf("invalid raster size for %s", path)
	}
	var gt [6]C.double
	if C.GDALGetGeoTransform(ds, &gt[0]) != 0 {
		return env, fmt.Errorf("no geotransform in %s", path)
	}
	wkt := C.GDALGetProjectionRef(ds)
	if wkt == nil || *wkt == 0 {
		return env, fmt.Errorf("no projection in %s", path)
	}
	src := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(src)
	if C.OSRImportFromWkt(src, &wkt) != 0 {
		return env, fmt.Errorf("projection parse failed for %s", path)
	}
	C._set_traditional_gis_order(src)
	dst := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(dst)
	C.OSRImportFromEPSG(dst, 4326)
	C._set_traditional_gis_order(dst)
	ct := C.OCTNewCoordinateTransformation(src, dst)
	if ct == nil {
		return env, fmt.Errorf("coordinate transform to WGS84 failed for %s", path)
	}
	defer C.OCTDestroyCoordinateTransformation(ct)
	corners := [][2]float64{{0, 0}, {float64(w), 0}, {0, float64(h)}, {float64(w), float64(h)}}
	var xs, ys [4]float64
	for i, pt := range corners {
		x := float64(gt[0]) + pt[0]*float64(gt[1]) + pt[1]*float64(gt[2])
		y := float64(gt[3]) + pt[0]*float64(gt[4]) + pt[1]*float64(gt[5])
		z := 0.0
		cx, cy, cz := C.double(x), C.double(y), C.double(z)
		if C.OCTTransform(ct, 1, &cx, &cy, &cz) == 0 {
			return env, fmt.Errorf("corner transform failed for %s", path)
		}
		xs[i], ys[i] = float64(cx), float64(cy)
	}
	env = [4]float64{xs[0], ys[0], xs[0], ys[0]}
	for i := 1; i < 4; i++ {
		if xs[i] < env[0] {
			env[0] = xs[i]
		}
		if ys[i] < env[1] {
			env[1] = ys[i]
		}
		if xs[i] > env[2] {
			env[2] = xs[i]
		}
		if ys[i] > env[3] {
			env[3] = ys[i]
		}
	}
	return env, nil
}

func dedupeTiles(entries []SafeTileEntry) ([]SafeTileEntry, int) {
	seen := make(map[string]SafeTileEntry, len(entries))
	for _, e := range entries {
		key := strings.TrimSpace(e.SceneID)
		if key == "" {
			key = strings.TrimSpace(e.TileID) + "|" + strings.TrimSpace(e.CapturedAt)
		}
		cur, ok := seen[key]
		if !ok || strings.TrimSpace(e.SafeName) > strings.TrimSpace(cur.SafeName) || strings.TrimSpace(e.ImgDataPath) > strings.TrimSpace(cur.ImgDataPath) {
			seen[key] = e
		}
	}
	out := make([]SafeTileEntry, 0, len(seen))
	for _, v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CapturedAt != out[j].CapturedAt {
			return out[i].CapturedAt < out[j].CapturedAt
		}
		if out[i].TileID != out[j].TileID {
			return out[i].TileID < out[j].TileID
		}
		return out[i].SafeName < out[j].SafeName
	})
	return out, len(entries) - len(out)
}

func buildSentinelCacheForYear(yearDir string) (*TileCache, error) {
	yearName := filepath.Base(yearDir)
	cache := &TileCache{Version: 9, Preprocess: "sentinel", Generated: time.Now().Format(time.RFC3339), Tiles: make([]SafeTileEntry, 0, 256)}
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return nil, err
	}
	indexed := 0
	skipped := 0
	for _, ent := range entries {
		if !ent.IsDir() || !strings.HasSuffix(ent.Name(), ".SAFE") {
			continue
		}
		safePath := filepath.Join(yearDir, ent.Name())
		dateOnly, capturedAt, tileID, sceneID, err := parseSAFEName(ent.Name())
		if err != nil {
			skipped++
			continue
		}
		imgData, err := findIMGData(safePath)
		if err != nil {
			skipped++
			continue
		}
		cloud, err := readTileCloud(filepath.Join(safePath, "MTD_MSIL1C.xml"))
		if err != nil {
			skipped++
			continue
		}
		ok, err := shouldIndexByB02Size(imgData, 70000000)
		if err != nil {
			skipped++
			continue
		}
		if !ok {
			skipped++
			continue
		}
		jp2, err := firstJP2InDir(imgData)
		if err != nil {
			skipped++
			continue
		}
		env, err := envelopeWGS84FromJP2(jp2)
		if err != nil {
			skipped++
			continue
		}
		cache.Tiles = append(cache.Tiles, SafeTileEntry{
			Date:        dateOnly,
			CapturedAt:  capturedAt,
			SceneID:     sceneID,
			SafeName:    ent.Name(),
			ImgDataPath: imgData,
			Cloud:       cloud,
			Envelope:    env,
			TileID:      tileID,
		})
		indexed++
	}
	cache.Tiles, _ = dedupeTiles(cache.Tiles)
	if y, err := time.Parse("2006", yearName); err == nil {
		cache.Year = y.Year()
	}
	log.Printf("index year=%s indexed=%d skipped=%d final=%d", yearName, indexed, skipped, len(cache.Tiles))
	return cache, nil
}

func buildSentinel2ACacheForYear(yearDir string) (*TileCache, error) {
	yearName := filepath.Base(yearDir)
	cache := &TileCache{Version: 9, Preprocess: "sentinel2a", Generated: time.Now().Format(time.RFC3339), Tiles: make([]SafeTileEntry, 0, 256)}
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return nil, err
	}
	indexed := 0
	skipped := 0
	for _, ent := range entries {
		if !ent.IsDir() || !strings.HasSuffix(ent.Name(), ".SAFE") {
			continue
		}
		safePath := filepath.Join(yearDir, ent.Name())
		dateOnly, capturedAt, tileID, sceneID, err := parseSAFEName(ent.Name())
		if err != nil {
			skipped++
			continue
		}
		imgData, err := findIMGData(safePath)
		if err != nil {
			skipped++
			continue
		}
		meta, err := readL2AMetadata(filepath.Join(safePath, "MTD_MSIL2A.xml"))
		if err != nil {
			skipped++
			continue
		}
		jp2, err := firstJP2InDir(imgData)
		if err != nil {
			skipped++
			continue
		}
		env, err := envelopeWGS84FromJP2(jp2)
		if err != nil {
			skipped++
			continue
		}
		nodata := meta.Nodata
		cache.Tiles = append(cache.Tiles, SafeTileEntry{
			Date:                  dateOnly,
			CapturedAt:            capturedAt,
			SceneID:               sceneID,
			SafeName:              ent.Name(),
			ImgDataPath:           imgData,
			Cloud:                 meta.Cloud,
			NodataPixelPercentage: &nodata,
			Envelope:              env,
			TileID:                tileID,
		})
		indexed++
	}
	cache.Tiles, _ = dedupeTiles(cache.Tiles)
	if y, err := time.Parse("2006", yearName); err == nil {
		cache.Year = y.Year()
	}
	log.Printf("index L2A year=%s indexed=%d skipped=%d final=%d", yearName, indexed, skipped, len(cache.Tiles))
	return cache, nil
}

func writeCache(path string, cache *TileCache) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(cache)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func collectYearDirs(root, yearArg string) ([]string, error) {
	y := strings.TrimSpace(yearArg)
	if y == "" {
		return nil, errors.New("--year is required; use --year YYYY or --year all")
	}
	if strings.EqualFold(y, "all") {
		dirs, err := filepath.Glob(filepath.Join(root, "20??"))
		if err != nil {
			return nil, err
		}
		out := make([]string, 0, len(dirs))
		for _, d := range dirs {
			if st, err := os.Stat(d); err == nil && st.IsDir() {
				out = append(out, d)
			}
		}
		sort.Strings(out)
		if len(out) == 0 {
			return nil, fmt.Errorf("year folders 20?? not found in %s", root)
		}
		return out, nil
	}
	if len(y) != 4 || !strings.HasPrefix(y, "20") {
		return nil, fmt.Errorf("invalid --year value: %s", y)
	}
	d := filepath.Join(root, y)
	st, err := os.Stat(d)
	if err != nil || !st.IsDir() {
		return nil, fmt.Errorf("year folder not found: %s", d)
	}
	return []string{d}, nil
}

func parseTimeAny(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02 15:04", "2006-01-02"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time format: %s", s)
}

func entryTimeID(v any, fallback string) (time.Time, string, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return time.Time{}, "", false
	}
	id := strings.TrimSpace(fallback)
	if sv, ok := m["id"].(string); ok && strings.TrimSpace(sv) != "" {
		id = strings.TrimSpace(sv)
	}
	st, ok := m["time"].(string)
	if !ok || strings.TrimSpace(st) == "" || id == "" {
		return time.Time{}, "", false
	}
	t, err := parseTimeAny(st)
	if err != nil {
		return time.Time{}, "", false
	}
	return t, id, true
}

func removeZip(resultsPath, id string) {
	p := filepath.Join(resultsPath, id+".zip")
	if err := os.Remove(p); err == nil {
		log.Printf("result zip removed: %s", p)
	} else if err != nil && !os.IsNotExist(err) {
		log.Printf("result zip remove error: %s: %v", p, err)
	}
}

func pruneResults(resultsFile, resultsPath string, days int) error {
	data, err := os.ReadFile(resultsFile)
	if err != nil {
		return err
	}
	var doc any
	if len(strings.TrimSpace(string(data))) == 0 {
		doc = map[string]any{}
	} else if err := json.Unmarshal(data, &doc); err != nil {
		return err
	}
	cutoff := time.Now().AddDate(0, 0, -days)
	removed := 0

	switch root := doc.(type) {
	case map[string]any:
		out := make(map[string]any, len(root))
		for k, v := range root {
			t, id, ok := entryTimeID(v, k)
			if ok && t.Before(cutoff) {
				removed++
				log.Printf("result removed: id=%s time=%s cutoff=%s", id, t.Format(time.RFC3339), cutoff.Format(time.RFC3339))
				removeZip(resultsPath, id)
				continue
			}
			out[k] = v
		}
		doc = out
	case []any:
		out := make([]any, 0, len(root))
		for _, v := range root {
			t, id, ok := entryTimeID(v, "")
			if ok && t.Before(cutoff) {
				removed++
				log.Printf("result removed: id=%s time=%s cutoff=%s", id, t.Format(time.RFC3339), cutoff.Format(time.RFC3339))
				removeZip(resultsPath, id)
				continue
			}
			out = append(out, v)
		}
		doc = out
	default:
		return fmt.Errorf("unsupported results JSON root type: %T", doc)
	}

	updated, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	if err := os.WriteFile(resultsFile, updated, 0o644); err != nil {
		return err
	}
	log.Printf("results cleanup completed: removed=%d cutoff=%s", removed, cutoff.Format(time.RFC3339))
	return nil
}

func main() {
	configPath := flag.String("config", defaultConfigPath(), "path to config.json")
	yearArg := flag.String("year", time.Now().Format("2006"), "year to index: YYYY or all")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	lf, err := setupLogger(cfg.Logs)
	if err != nil {
		log.Fatal(err)
	}
	defer lf.Close()

	log.Printf("cron started")

	yearDirs, err := collectYearDirs(cfg.Sentinel, *yearArg)
	if err != nil {
		log.Fatal(err)
	}
	for _, yearDir := range yearDirs {
		cache, err := buildSentinelCacheForYear(yearDir)
		if err != nil {
			log.Printf("index error: %v", err)
			continue
		}
		cashPath := filepath.Join(yearDir, "cash.json")
		if err := writeCache(cashPath, cache); err != nil {
			log.Printf("cash write error: %v", err)
			continue
		}
		log.Printf("cash written: path=%s tiles=%d preprocess=%s", cashPath, len(cache.Tiles), cache.Preprocess)
	}

	if strings.TrimSpace(cfg.Sentinel2A) != "" {
		yearDirsL2A, err := collectYearDirs(cfg.Sentinel2A, *yearArg)
		if err != nil {
			log.Printf("index L2A skipped: %v", err)
		} else {
			for _, yearDir := range yearDirsL2A {
				cache, err := buildSentinel2ACacheForYear(yearDir)
				if err != nil {
					log.Printf("index L2A error: %v", err)
					continue
				}
				cashPath := filepath.Join(yearDir, "cash.json")
				if err := writeCache(cashPath, cache); err != nil {
					log.Printf("cash write error: %v", err)
					continue
				}
				log.Printf("cash written: path=%s tiles=%d preprocess=%s", cashPath, len(cache.Tiles), cache.Preprocess)
			}
		}
	}

	if err := pruneResults(cfg.ResultsFile, cfg.ResultsPath, 30); err != nil {
		log.Printf("results cleanup error: %v", err)
	}

	log.Printf("cron finished")
}
