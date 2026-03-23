package main

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func clip01(x float32) float32 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}

func preprocessBySpec(v float32, spec ModelSpec) float32 {
	if v < 0 {
		v = 0
	}
	if v > 2 && spec.Divisor > 0 {
		v = v / spec.Divisor
	}
	return clip01(v)
}

type ProcessConfig struct {
	App           *AppConfig
	Spec          ModelSpec
	Start         string
	End           string
	Start2        string
	End2          string
	Cloud         float64
	SearchPolygon string
	BatchSize     int
	Device        string
	CudaDeviceID  int
	MinArea       float64
	Simplify      float64
	OutputName    string
	CashPath      string
}

type ProcessStats struct {
	PrimaryCandidates   int
	SecondaryCandidates int
	TileFootprintsSeen  int
	TilesProcessed      int
	TilesSkippedOutside int
	TilesReadErrors     int
	ModelRuns           int
	ModelErrors         int
	SecondaryMisses     int
	ResultPolygons      int
}

type periodRunResult struct {
	shpPath string
	stats   ProcessStats
}

type progressLogger struct {
	total       int
	stepPercent int
	nextPercent int
	started     time.Time
}

func newProgressLogger(total int) *progressLogger {
	return &progressLogger{
		total:       total,
		stepPercent: 10,
		nextPercent: 10,
		started:     time.Now(),
	}
}

func formatDurationMMSS(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	minutes := int(d.Minutes())
	seconds := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d", minutes, seconds)
}

func (p *progressLogger) Update(done int) {
	if p == nil || p.total <= 0 || done <= 0 {
		return
	}
	percent := int(float64(done) * 100.0 / float64(p.total))
	if done >= p.total {
		percent = 100
	}
	for percent >= p.nextPercent && p.nextPercent <= 100 {
		elapsed := time.Since(p.started)
		avg := elapsed / time.Duration(done)
		remaining := p.total - done
		eta := avg * time.Duration(remaining)
		log.Printf("Tiles %d/%d. %d%%. Time: %s. ETA: %s", done, p.total, p.nextPercent, formatDurationMMSS(avg), formatDurationMMSS(eta))
		p.nextPercent += p.stepPercent
	}
}

func collectAllowedTiles(tiles []SafeTile, allowedGeom Geometry, allowedEnv Envelope) ([]SafeTile, int) {
	allowed := make([]SafeTile, 0, len(tiles))
	skipped := 0
	for _, tile := range tiles {
		if !tileAllowed(tile, allowedGeom, allowedEnv) {
			skipped++
			continue
		}
		allowed = append(allowed, tile)
	}
	return allowed, skipped
}

func tileNamesForLog(tiles []SafeTile) string {
	if len(tiles) == 0 {
		return ""
	}
	names := make([]string, 0, len(tiles))
	for _, tile := range tiles {
		names = append(names, tileLabel(tile))
	}
	sort.Strings(names)
	return strings.Join(names, ", ")
}

func RunProcess(cfg ProcessConfig) (string, ProcessStats, error) {
	allowedGeom, err := BuildAllowedGeometry(cfg.App, cfg.SearchPolygon, cfg.Spec.Poligons)
	if err != nil {
		return "", ProcessStats{}, err
	}
	defer DestroyGeometry(allowedGeom)

	workDir, err := os.MkdirTemp(cfg.App.Tmp, "sentinel-work-")
	if err != nil {
		return "", ProcessStats{}, err
	}
	defer os.RemoveAll(workDir)

	var finalShp string
	var stats ProcessStats
	if cfg.Spec.Inputs > 1 {
		res, err := runPairedPeriods(cfg, allowedGeom, workDir)
		if err != nil {
			return "", ProcessStats{}, err
		}
		finalShp = res.shpPath
		stats = res.stats
	} else {
		res, err := runSinglePeriod(cfg, allowedGeom, cfg.Start, cfg.End, cfg.Cloud, workDir)
		if err != nil {
			return "", ProcessStats{}, err
		}
		finalShp = res.shpPath
		stats = res.stats
	}

	outDir, err := os.MkdirTemp(cfg.App.Tmp, "sentinel-out-")
	if err != nil {
		return "", stats, err
	}
	defer os.RemoveAll(outDir)
	if err := os.MkdirAll(filepath.Join(outDir, "Shape"), 0o755); err != nil {
		return "", stats, err
	}

	if finalShp == "" {
		return "", stats, fmt.Errorf("no polygons found for output")
	}
	count, err := CountFeaturesInVector(finalShp)
	if err != nil {
		return "", stats, err
	}
	stats.ResultPolygons = count
	if stats.ResultPolygons == 0 {
		return "", stats, fmt.Errorf("no polygons found for output")
	}
	shpOut := filepath.Join(outDir, "Shape", cfg.Spec.Name+".shp")
	if err := CopyShapefileSet(finalShp, shpOut); err != nil {
		return "", stats, err
	}
	geojsonOut := filepath.Join(outDir, cfg.Spec.Name+".geojson")
	if err := ConvertShapefileToGeoJSON(finalShp, geojsonOut); err != nil {
		return "", stats, err
	}

	zipPath := filepath.Join(cfg.App.ResultsPath, cfg.OutputName+".zip")
	if err := zipDir(outDir, zipPath); err != nil {
		return "", stats, err
	}
	return zipPath, stats, nil
}

func runSinglePeriod(cfg ProcessConfig, allowedGeom Geometry, start, end string, cloud float64, workDir string) (periodRunResult, error) {
	primaryTiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, start, end, cloud)
	if err != nil {
		return periodRunResult{}, err
	}
	stats := ProcessStats{PrimaryCandidates: len(primaryTiles)}
	if len(primaryTiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no Sentinel-2 tiles found in period %s..%s", start, end)
	}

	sess, err := NewORTSession(cfg.App.ModelPath(cfg.Spec), cfg.Device, cfg.CudaDeviceID)
	if err != nil {
		return periodRunResult{}, err
	}
	defer sess.Close()

	mergedShp := filepath.Join(workDir, fmt.Sprintf("%s_single_merged.shp", cfg.Spec.Name))
	_ = os.Remove(mergedShp)
	allowedEnv := GeometryEnvelope(allowedGeom)
	filteredTiles, skippedOutside := collectAllowedTiles(primaryTiles, allowedGeom, allowedEnv)
	stats.TileFootprintsSeen = len(primaryTiles)
	stats.TilesSkippedOutside = skippedOutside
	log.Printf("tiles selected for processing: count=%d names=[%s]", len(filteredTiles), tileNamesForLog(filteredTiles))
	progress := newProgressLogger(len(filteredTiles))
	doneTiles := 0
	for _, tile := range filteredTiles {
		cube1, err := ReadSentinelTileCube(tile, cfg.Spec.Channels)
		if err != nil {
			stats.TilesReadErrors++
			log.Printf("tile read error %s: %v", tileLabel(tile), err)
			doneTiles++
			progress.Update(doneTiles)
			continue
		}
		stats.TilesProcessed++
		stats.ModelRuns++
		shp, err := RunModelOnCubes([]*RasterCube{cube1}, sess, cfg.BatchSize, cfg.Spec, workDir, cfg.MinArea, cfg.Simplify)
		if err != nil {
			stats.ModelErrors++
			log.Printf("model error %s: %v", tileLabel(tile), err)
			doneTiles++
			progress.Update(doneTiles)
			continue
		}
		if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
			log.Printf("merge error %s: %v", tileLabel(tile), err)
		}
		CleanupShapefileSet(shp)
		doneTiles++
		progress.Update(doneTiles)
	}
	if _, err := os.Stat(mergedShp); err != nil {
		return periodRunResult{stats: stats}, fmt.Errorf("no polygons found for period %s..%s", start, end)
	}
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
}

func runPairedPeriods(cfg ProcessConfig, allowedGeom Geometry, workDir string) (periodRunResult, error) {
	newTiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, cfg.Start, cfg.End, cfg.Cloud)
	if err != nil {
		return periodRunResult{}, err
	}
	baseTiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, cfg.Start2, cfg.End2, cfg.Cloud)
	if err != nil {
		return periodRunResult{}, err
	}
	stats := ProcessStats{PrimaryCandidates: len(newTiles), SecondaryCandidates: len(baseTiles)}
	if len(newTiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no Sentinel-2 tiles found in primary period %s..%s", cfg.Start, cfg.End)
	}
	if len(baseTiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no Sentinel-2 tiles found in secondary period %s..%s", cfg.Start2, cfg.End2)
	}

	baseByTile := make(map[string][]SafeTile)
	for _, tile := range baseTiles {
		baseByTile[tile.TileID] = append(baseByTile[tile.TileID], tile)
	}
	for id := range baseByTile {
		sort.Slice(baseByTile[id], func(i, j int) bool {
			return tileTime(baseByTile[id][i]).Before(tileTime(baseByTile[id][j]))
		})
	}

	type tilePair struct{ older, newer SafeTile }
	selectedPairs := make([]tilePair, 0)
	selectedNames := make([]string, 0)
	seenPairs := make(map[string]struct{})
	allowedEnv := GeometryEnvelope(allowedGeom)

	for _, newer := range newTiles {
		stats.TileFootprintsSeen++
		if !tileAllowed(newer, allowedGeom, allowedEnv) {
			stats.TilesSkippedOutside++
			continue
		}
		older, ok := pickBaseTileForNewer(newer, baseByTile[newer.TileID])
		if !ok {
			stats.SecondaryMisses++
			continue
		}
		stats.TileFootprintsSeen++
		if !tileAllowed(older, allowedGeom, allowedEnv) {
			stats.TilesSkippedOutside++
			continue
		}
		pairKey := sceneKey(newer) + "||" + sceneKey(older)
		if _, dup := seenPairs[pairKey]; dup {
			continue
		}
		seenPairs[pairKey] = struct{}{}
		selectedPairs = append(selectedPairs, tilePair{older: older, newer: newer})
		selectedNames = append(selectedNames, fmt.Sprintf("%s [%s -> %s]", newer.TileID, older.Date, newer.Date))
	}
	if len(selectedPairs) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no valid Sentinel-2 tile pairs between periods")
	}
	sort.Strings(selectedNames)
	log.Printf("tile pairs selected for processing: count=%d names=[%s]", len(selectedPairs), strings.Join(selectedNames, ", "))

	sess, err := NewORTSession(cfg.App.ModelPath(cfg.Spec), cfg.Device, cfg.CudaDeviceID)
	if err != nil {
		return periodRunResult{}, err
	}
	defer sess.Close()

	mergedShp := filepath.Join(workDir, fmt.Sprintf("%s_paired_merged.shp", cfg.Spec.Name))
	_ = os.Remove(mergedShp)
	progress := newProgressLogger(len(selectedPairs) * 2)
	doneTiles := 0
	for _, pair := range selectedPairs {
		older := pair.older
		newer := pair.newer

		cubeOlder, err := ReadSentinelTileCube(older, cfg.Spec.Channels)
		if err != nil {
			stats.TilesReadErrors++
			log.Printf("tile read error %s: %v", tileLabel(older), err)
			doneTiles += 2
			progress.Update(doneTiles)
			continue
		}
		cubeNewer, err := ReadSentinelTileCube(newer, cfg.Spec.Channels)
		if err != nil {
			stats.TilesReadErrors++
			log.Printf("tile read error %s: %v", tileLabel(newer), err)
			doneTiles += 2
			progress.Update(doneTiles)
			continue
		}
		cubeOlder, cubeNewer, err = alignedIntersectionCubes(cubeOlder, cubeNewer)
		if err != nil {
			stats.TilesReadErrors++
			log.Printf("pair alignment error %s/%s: %v", tileLabel(older), tileLabel(newer), err)
			doneTiles += 2
			progress.Update(doneTiles)
			continue
		}
		stats.TilesProcessed += 2
		stats.ModelRuns++

		shp, err := RunModelOnCubes([]*RasterCube{cubeOlder, cubeNewer}, sess, cfg.BatchSize, cfg.Spec, workDir, cfg.MinArea, cfg.Simplify)
		if err != nil {
			stats.ModelErrors++
			log.Printf("model error %s/%s: %v", tileLabel(older), tileLabel(newer), err)
			doneTiles += 2
			progress.Update(doneTiles)
			continue
		}
		if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
			log.Printf("merge error %s/%s: %v", tileLabel(older), tileLabel(newer), err)
		}
		CleanupShapefileSet(shp)
		doneTiles += 2
		progress.Update(doneTiles)
	}
	if _, err := os.Stat(mergedShp); err != nil {
		return periodRunResult{stats: stats}, fmt.Errorf("no polygons found for paired periods")
	}
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
}

func pickBaseTileForNewer(newer SafeTile, baseCandidates []SafeTile) (SafeTile, bool) {
	if len(baseCandidates) == 0 {
		return SafeTile{}, false
	}
	newerTime := tileTime(newer)
	var picked SafeTile
	found := false
	for _, base := range baseCandidates {
		baseTime := tileTime(base)
		if newerTime.Sub(baseTime) < 24*time.Hour {
			continue
		}
		if !found || baseTime.After(tileTime(picked)) {
			picked = base
			found = true
		}
	}
	return picked, found
}

func loadTilesForCfg(cashPath, sentinelRoot, start, end string, cloud float64) ([]SafeTile, error) {
	if strings.HasSuffix(strings.ToLower(strings.TrimSpace(cashPath)), ".json") {
		return LoadTilesFromSingleCash(cashPath, start, end, cloud)
	}
	return LoadTilesForPeriod(sentinelRoot, start, end, cloud)
}

func tileAllowed(tile SafeTile, allowedGeom Geometry, allowedEnv Envelope) bool {
	if !EnvelopeIntersects(allowedEnv, tile.Envelope) {
		return false
	}
	tileFootprint := TileFootprintFromCache(tile)
	if tileFootprint == nil {
		return false
	}
	defer DestroyGeometry(tileFootprint)
	return GeometryIntersects(tileFootprint, allowedGeom)
}

func RunModelOnCubes(cubes []*RasterCube, sess *ORTSession, batchSize int, spec ModelSpec, tmpDir string, minArea, simplify float64) (string, error) {
	if len(cubes) == 0 {
		return "", errors.New("no input cubes")
	}
	base := cubes[0]
	img := combineInputs(cubes)
	validMask := combineValidMasks(cubes)
	if len(img) == 0 || len(img[0]) == 0 || len(img[0][0]) == 0 || len(img[0][0][0]) == 0 {
		return "", errors.New("empty input image")
	}
	h, w := len(img[0][0]), len(img[0][0][0])
	out := make([]float32, h*w)
	tileH, tileW, bound := spec.Tile, spec.Tile, spec.Bound
	stepY, stepX := tileH-2*bound, tileW-2*bound
	if stepY <= 0 || stepX <= 0 {
		return "", fmt.Errorf("invalid bound=%d for tile=%d", bound, tileH)
	}
	inChannels := len(img)

	type tileMeta struct{ dstY0, dstY1, dstX0, dstX1, srcY0, srcX0 int }
	var metas []tileMeta
	var batchInput []float32
	var batchValid []uint8

	flush := func() error {
		if len(metas) == 0 {
			return nil
		}
		preds, err := sess.Predict(batchInput, len(metas), inChannels, tileH, tileW, 1)
		if err != nil {
			return err
		}
		pixelCount := tileH * tileW
		for bi, m := range metas {
			baseOff := bi * pixelCount
			for yy := m.dstY0; yy < m.dstY1; yy++ {
				for xx := m.dstX0; xx < m.dstX1; xx++ {
					py, px := (yy-m.dstY0)+m.srcY0, (xx-m.dstX0)+m.srcX0
					pOff := py*tileW + px
					vIdx := bi*pixelCount + pOff
					if batchValid[vIdx] == 0 {
						out[yy*w+xx] = 0
						continue
					}
					if preds[baseOff+pOff] > spec.Threshold {
						out[yy*w+xx] = 1
					} else {
						out[yy*w+xx] = 0
					}
				}
			}
		}
		metas, batchInput, batchValid = metas[:0], batchInput[:0], batchValid[:0]
		return nil
	}

	for y0 := 0; y0 < h; y0 += stepY {
		for x0 := 0; x0 < w; x0 += stepX {
			patch := make([]float32, inChannels*tileH*tileW)
			valid := make([]uint8, tileH*tileW)
			for yy := 0; yy < tileH; yy++ {
				sy := y0 + yy
				if sy >= h {
					continue
				}
				for xx := 0; xx < tileW; xx++ {
					sx := x0 + xx
					if sx >= w {
						continue
					}
					pixIdx := sy*w + sx
					if len(validMask) > 0 && validMask[pixIdx] == 0 {
						continue
					}
					for c := 0; c < inChannels; c++ {
						val := preprocessBySpec(img[c][0][sy][sx], spec)
						patch[c*tileH*tileW+yy*tileW+xx] = val
					}
					valid[yy*tileW+xx] = 1
				}
			}
			srcY0, srcY1 := bound, tileH-bound
			srcX0, srcX1 := bound, tileW-bound
			if y0 == 0 {
				srcY0 = 0
			}
			if x0 == 0 {
				srcX0 = 0
			}
			if y0+tileH >= h {
				srcY1 = h - y0
			}
			if x0+tileW >= w {
				srcX1 = w - x0
			}
			if srcY1 < srcY0 {
				srcY1 = srcY0
			}
			if srcX1 < srcX0 {
				srcX1 = srcX0
			}
			dstY0, dstY1 := y0+srcY0, min(y0+srcY1, h)
			dstX0, dstX1 := x0+srcX0, min(x0+srcX1, w)

			metas = append(metas, tileMeta{dstY0: dstY0, dstY1: dstY1, dstX0: dstX0, dstX1: dstX1, srcY0: srcY0, srcX0: srcX0})
			batchInput = append(batchInput, patch...)
			batchValid = append(batchValid, valid...)

			if len(metas) >= batchSize {
				if err := flush(); err != nil {
					return "", err
				}
			}
		}
	}
	if err := flush(); err != nil {
		return "", err
	}

	maskTif := filepath.Join(tmpDir, spec.Name+"_mask.tif")
	if err := WriteGeoTIFF1(maskTif, out, w, h, base.Geo, base.Proj); err != nil {
		return "", err
	}
	defer os.Remove(maskTif)
	return PolygonizeMask(maskTif, 1, minArea, simplify)
}

func combineValidMasks(cubes []*RasterCube) []uint8 {
	if len(cubes) == 0 {
		return nil
	}
	base := cubes[0]
	if len(base.ValidMask) == 0 {
		return nil
	}
	out := make([]uint8, len(base.ValidMask))
	copy(out, base.ValidMask)
	for _, cube := range cubes[1:] {
		if len(cube.ValidMask) != len(out) {
			continue
		}
		for i := range out {
			if cube.ValidMask[i] == 0 {
				out[i] = 0
			}
		}
	}
	return out
}

func combineInputs(cubes []*RasterCube) [][][][]float32 {
	var all [][][][]float32
	for _, c := range cubes {
		all = append(all, c.Data...)
	}
	return all
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func zipDir(srcDir, zipPath string) error {
	zf, err := os.Create(zipPath)
	if err != nil {
		return err
	}
	defer zf.Close()
	zw := zip.NewWriter(zf)
	defer zw.Close()

	return filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		w, err := zw.Create(rel)
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(w, f)
		return err
	})
}


func CopyShapefileSet(srcShp, dstShp string) error {
	srcBase := strings.TrimSuffix(srcShp, filepath.Ext(srcShp))
	dstBase := strings.TrimSuffix(dstShp, filepath.Ext(dstShp))
	if err := os.MkdirAll(filepath.Dir(dstShp), 0o755); err != nil {
		return err
	}
	for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
		src := srcBase + ext
		if _, err := os.Stat(src); err != nil {
			if ext == ".cpg" || ext == ".prj" {
				continue
			}
			return err
		}
		dst := dstBase + ext
		in, err := os.Open(src)
		if err != nil { return err }
		out, err := os.Create(dst)
		if err != nil { in.Close(); return err }
		if _, err := io.Copy(out, in); err != nil {
			out.Close(); in.Close(); return err
		}
		out.Close(); in.Close()
	}
	return nil
}
