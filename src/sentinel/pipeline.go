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
    // identity: данные уже в правильном диапазоне, не трогаем.
    mode := strings.ToLower(strings.TrimSpace(spec.Preprocess))
    if mode == "identity" {
	return v
    }
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

// selectBestScenePerTile выбирает для каждого tile_id один снимок:
// с минимальной облачностью; при равной — самый поздний по дате.
// Это воспроизводит логику kosmo: из нескольких снимков одного тайла
// за период строилась мозаика, а не прогонялся каждый независимо.
// Один снимок на тайл устраняет артефакты от union разных дат.
func selectBestScenePerTile(tiles []SafeTile) []SafeTile {
    best := make(map[string]SafeTile)
    for _, tile := range tiles {
	cur, ok := best[tile.TileID]
	if !ok {
	    best[tile.TileID] = tile
	    continue
	}
	// Предпочитаем меньше облаков; при равной облачности — более позднюю дату.
	if tile.Cloud < cur.Cloud ||
	    (tile.Cloud == cur.Cloud && tileTime(tile).After(tileTime(cur))) {
	    best[tile.TileID] = tile
	}
    }
    out := make([]SafeTile, 0, len(best))
    for _, tile := range best {
	out = append(out, tile)
    }
    sortTiles(out)
    return out
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

func isNDVITimeSeriesSpec(spec ModelSpec) bool {
    if strings.ToLower(strings.TrimSpace(spec.Preprocess)) != "identity" {
	return false
    }
    if len(spec.Channels) == 0 {
	return false
    }
    for _, ch := range spec.Channels {
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(ch)), "NDVI") {
	    return false
	}
    }
    return true
}

func runNDVITimeSeriesPeriod(cfg ProcessConfig, allowedGeom Geometry, start, end string, cloud float64, workDir string) (periodRunResult, error) {
    primaryTiles, err := loadTilesForCfg(cfg.CashPath, rootForSpec(cfg.App, cfg.Spec), start, end, cloud)
    if err != nil {
	return periodRunResult{}, err
    }
    stats := ProcessStats{PrimaryCandidates: len(primaryTiles)}
    if len(primaryTiles) == 0 {
	return periodRunResult{stats: stats}, fmt.Errorf("no Sentinel-2 tiles found in period %s..%s", start, end)
    }
    allowedEnv := GeometryEnvelope(allowedGeom)
    filteredTiles, skippedOutside := collectAllowedTiles(primaryTiles, allowedGeom, allowedEnv)
    stats.TileFootprintsSeen = len(primaryTiles)
    stats.TilesSkippedOutside = skippedOutside
    byTile := make(map[string][]SafeTile)
    for _, tile := range filteredTiles {
	byTile[tile.TileID] = append(byTile[tile.TileID], tile)
    }
    selectedIDs := make([]string, 0, len(byTile))
    for id := range byTile {
	selectedIDs = append(selectedIDs, id)
    }
    sort.Strings(selectedIDs)
    selectedNames := make([]string, 0, len(selectedIDs))
    for _, id := range selectedIDs {
	latest := byTile[id][0]
	for _, t := range byTile[id][1:] {
	    if tileTime(t).After(tileTime(latest)) {
		latest = t
	    }
	}
	selectedNames = append(selectedNames, tileLabel(latest))
    }
    log.Printf("tiles selected for processing: count=%d names=[%s]", len(selectedIDs), strings.Join(selectedNames, ", "))

    sess, err := NewORTSession(cfg.App.ModelPath(cfg.Spec), cfg.Device, cfg.CudaDeviceID)
    if err != nil {
	return periodRunResult{}, err
    }
    defer sess.Close()

    mergedShp := filepath.Join(workDir, fmt.Sprintf("%s_single_merged.shp", cfg.Spec.Name))
    _ = os.Remove(mergedShp)
    progress := newProgressLogger(len(selectedIDs))
    doneTiles := 0
    for _, id := range selectedIDs {
	cube, err := BuildNDVITimeSeriesCube(byTile[id], cfg.Spec, start, end)
	if err != nil {
	    stats.TilesReadErrors++
	    log.Printf("tile read error %s: %v", id, err)
	    doneTiles++
	    progress.Update(doneTiles)
	    continue
	}
	stats.TilesProcessed++
	stats.ModelRuns++
	shp, err := RunModelOnCubes([]*RasterCube{cube}, sess, cfg.BatchSize, cfg.Spec, workDir, cfg.MinArea, cfg.Simplify)
	if err != nil {
	    stats.ModelErrors++
	    log.Printf("model error %s: %v", id, err)
	    doneTiles++
	    progress.Update(doneTiles)
	    continue
	}
	if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
	    log.Printf("merge error %s: %v", id, err)
	}
	CleanupShapefileSet(shp)
	doneTiles++
	progress.Update(doneTiles)
    }
    if _, err := os.Stat(mergedShp); err != nil {
	return periodRunResult{stats: stats}, fmt.Errorf("no polygons found for period %s..%s", start, end)
    }
    if !cfg.Spec.Merge {
	log.Printf("dissolve skipped (merge=false): %s", cfg.Spec.Name)
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
    }
    log.Printf("dissolve start: %s", cfg.Spec.Name)
    dissolvedShp := filepath.Join(workDir, fmt.Sprintf("%s_single_dissolved.shp", cfg.Spec.Name))
    if err := DissolveOverlappingPolygons(mergedShp, dissolvedShp); err != nil {
	log.Printf("dissolve warning (using undissolved): %v", err)
	dissolvedShp = mergedShp
    }
    log.Printf("dissolve done: %s", cfg.Spec.Name)
    return periodRunResult{shpPath: dissolvedShp, stats: stats}, nil
}

func RunProcess(cfg ProcessConfig) (string, ProcessStats, error) {
    allowedGeom, err := BuildAllowedGeometry(cfg.App, cfg.SearchPolygon, cfg.Spec.PoligonsOn, cfg.Spec.PoligonsOff)
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
    if strings.EqualFold(cfg.Spec.Preprocess, "pathology_diff") {
	// Сравнение год к году: только новые очаги, которых не было в базовом периоде.
	res, err := runForestPathologyDiffPeriod(cfg, allowedGeom, workDir)
	if err != nil {
	    return "", ProcessStats{}, err
	}
	finalShp = res.shpPath
	stats = res.stats
    } else if strings.EqualFold(cfg.Spec.Preprocess, "pathology") {
	// Статистический алгоритм VOG1: воспроизводит kosmo PathologyPipeline.
	// Нейронная сеть не используется, ONNX не нужен.
	res, err := runForestPathologyPeriod(cfg, allowedGeom, workDir)
	if err != nil {
	    return "", ProcessStats{}, err
	}
	finalShp = res.shpPath
	stats = res.stats
    } else if cfg.Spec.Inputs > 1 {
	res, err := runPairedPeriods(cfg, allowedGeom, workDir)
	if err != nil {
	    return "", ProcessStats{}, err
	}
	finalShp = res.shpPath
	stats = res.stats
    } else {
	var res periodRunResult
	if isNDVITimeSeriesSpec(cfg.Spec) {
	    res, err = runNDVITimeSeriesPeriod(cfg, allowedGeom, cfg.Start, cfg.End, cfg.Cloud, workDir)
	} else {
	    res, err = runSinglePeriod(cfg, allowedGeom, cfg.Start, cfg.End, cfg.Cloud, workDir)
	}
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
    primaryTiles, err := loadTilesForCfg(cfg.CashPath, rootForSpec(cfg.App, cfg.Spec), start, end, cloud)
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
    if cfg.Spec.BestScenePerTile {
	before := len(filteredTiles)
	filteredTiles = selectBestScenePerTile(filteredTiles)
	log.Printf("best_scene_per_tile: reduced %d -> %d tiles", before, len(filteredTiles))
    }
    log.Printf("tiles selected for processing: count=%d names=[%s]", len(filteredTiles), tileNamesForLog(filteredTiles))
    progress := newProgressLogger(len(filteredTiles))
    doneTiles := 0
    for _, tile := range filteredTiles {
	cube1, err := ReadSentinelTileCubeWithSpec(tile, cfg.Spec.Channels, cfg.Spec.CloudMaskMode, cfg.Spec)
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
	nBeforeClip := CountShapefileFeatures(shp)
	log.Printf("tile polygons after model %s: %d features (min_area=%.0f)", tileLabel(tile), nBeforeClip, cfg.MinArea)
	if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
	    log.Printf("merge error %s: %v", tileLabel(tile), err)
	}
	nMerged := CountShapefileFeatures(mergedShp)
	log.Printf("tile polygons after clip %s: merged total=%d", tileLabel(tile), nMerged)
	CleanupShapefileSet(shp)
	doneTiles++
	progress.Update(doneTiles)
    }
    if _, err := os.Stat(mergedShp); err != nil {
	return periodRunResult{stats: stats}, fmt.Errorf("no polygons found for period %s..%s", start, end)
    }
    if !cfg.Spec.Merge {
	log.Printf("dissolve skipped (merge=false): %s", cfg.Spec.Name)
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
    }
    // Dissolve: объединяем перекрывающиеся полигоны на стыках Sentinel-тайлов.
    // Без этого на стыке двух тайлов один и тот же участок земли может быть
    // покрыт полигонами от обоих тайлов -> видны "квадратные" артефакты.
    log.Printf("dissolve start: %s", cfg.Spec.Name)
    dissolvedShp := filepath.Join(workDir, fmt.Sprintf("%s_single_dissolved.shp", cfg.Spec.Name))
    if err := DissolveOverlappingPolygons(mergedShp, dissolvedShp); err != nil {
	log.Printf("dissolve warning (using undissolved): %v", err)
	dissolvedShp = mergedShp
    }
    log.Printf("dissolve done: %s", cfg.Spec.Name)
    return periodRunResult{shpPath: dissolvedShp, stats: stats}, nil
}

func runPairedPeriods(cfg ProcessConfig, allowedGeom Geometry, workDir string) (periodRunResult, error) {
    newTiles, err := loadTilesForCfg(cfg.CashPath, rootForSpec(cfg.App, cfg.Spec), cfg.Start, cfg.End, cfg.Cloud)
    if err != nil {
	return periodRunResult{}, err
    }
    baseTiles, err := loadTilesForCfg(cfg.CashPath, rootForSpec(cfg.App, cfg.Spec), cfg.Start2, cfg.End2, cfg.Cloud)
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

	cubeOlder, err := ReadSentinelTileCubeWithSpec(older, cfg.Spec.Channels, cfg.Spec.CloudMaskMode, cfg.Spec)
	if err != nil {
	    stats.TilesReadErrors++
	    log.Printf("tile read error %s: %v", tileLabel(older), err)
	    doneTiles += 2
	    progress.Update(doneTiles)
	    continue
	}
	cubeNewer, err := ReadSentinelTileCubeWithSpec(newer, cfg.Spec.Channels, cfg.Spec.CloudMaskMode, cfg.Spec)
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

	var shp string
	switch cfg.Spec.effectivePairMode() {
	case "newer_only":
	    // Модель принимает один снимок; второй период используется только
	    // для выбора базовой сцены (pickBaseTileForNewer), в inference не идёт.
	    shp, err = RunModelOnCubes([]*RasterCube{cubeNewer}, sess, cfg.BatchSize, cfg.Spec, workDir, cfg.MinArea, cfg.Simplify)
	    if err != nil {
		stats.ModelErrors++
		log.Printf("model error %s: %v", tileLabel(newer), err)
		doneTiles += 2
		progress.Update(doneTiles)
		continue
	    }
	    if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
		log.Printf("merge error %s: %v", tileLabel(newer), err)
	    }
	    CleanupShapefileSet(shp)
	case "union":
	    // Каждый снимок прогоняется через модель независимо,
	    // результаты объединяются union-ом полигонов.
	    // Используем отдельные подкаталоги чтобы избежать коллизии имён файлов.
	    workOlder := filepath.Join(workDir, "older")
	    workNewer := filepath.Join(workDir, "newer")
	    _ = os.MkdirAll(workOlder, 0o755)
	    _ = os.MkdirAll(workNewer, 0o755)
	    shpOlder, err1 := RunModelOnCubes([]*RasterCube{cubeOlder}, sess, cfg.BatchSize, cfg.Spec, workOlder, cfg.MinArea, cfg.Simplify)
	    shpNewer, err2 := RunModelOnCubes([]*RasterCube{cubeNewer}, sess, cfg.BatchSize, cfg.Spec, workNewer, cfg.MinArea, cfg.Simplify)
	    switch {
	    case err1 != nil && err2 != nil:
		err = fmt.Errorf("both cubes failed: older=%v newer=%v", err1, err2)
	    case err1 == nil && err2 == nil:
		if err := AppendShapefileFeaturesClipped(shpOlder, allowedGeom, mergedShp); err != nil {
		    log.Printf("merge error older %s: %v", tileLabel(older), err)
		}
		CleanupShapefileSet(shpOlder)
		if err := AppendShapefileFeaturesClipped(shpNewer, allowedGeom, mergedShp); err != nil {
		    log.Printf("merge error newer %s: %v", tileLabel(newer), err)
		}
		CleanupShapefileSet(shpNewer)
	    case err1 == nil:
		shp = shpOlder
		CleanupShapefileSet(shpNewer)
	    default:
		shp = shpNewer
		CleanupShapefileSet(shpOlder)
	    }
	    if err != nil {
		stats.ModelErrors++
		log.Printf("model error %s/%s: %v", tileLabel(older), tileLabel(newer), err)
		doneTiles += 2
		progress.Update(doneTiles)
		continue
	    }
	    if shp != "" {
		if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
		    log.Printf("merge error %s: %v", tileLabel(older), err)
		}
		CleanupShapefileSet(shp)
	    }
	default: // "concat"
	    // Каналы обоих снимков конкатенируются и подаются в модель единым тензором.
	    shp, err = RunModelOnCubes([]*RasterCube{cubeOlder, cubeNewer}, sess, cfg.BatchSize, cfg.Spec, workDir, cfg.MinArea, cfg.Simplify)
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
	}
	doneTiles += 2
	progress.Update(doneTiles)
    }
    if _, err := os.Stat(mergedShp); err != nil {
	return periodRunResult{stats: stats}, fmt.Errorf("no polygons found for paired periods")
    }
    if !cfg.Spec.Merge {
	log.Printf("dissolve skipped (merge=false): %s", cfg.Spec.Name)
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
    }
    log.Printf("dissolve start: %s", cfg.Spec.Name)
    dissolvedShp := filepath.Join(workDir, fmt.Sprintf("%s_paired_dissolved.shp", cfg.Spec.Name))
    if err := DissolveOverlappingPolygons(mergedShp, dissolvedShp); err != nil {
	log.Printf("dissolve warning (using undissolved): %v", err)
	dissolvedShp = mergedShp
    }
    log.Printf("dissolve done: %s", cfg.Spec.Name)
    return periodRunResult{shpPath: dissolvedShp, stats: stats}, nil
}

func pickBaseTileForNewer(newer SafeTile, baseCandidates []SafeTile) (SafeTile, bool) {
    if len(baseCandidates) == 0 {
	return SafeTile{}, false
    }
    // Воспроизводим логику kosmo (_get_base_image):
    // 1. Фильтруем: base строго раньше newer (хотя бы на 1 день)
    // 2. Сортируем по дате descending → берём первый (самый свежий)
    // Если строго ранних нет (периоды пересекаются или одинаковые) —
    // берём самый свежий из всех (best-effort).
    var candidates []SafeTile
    for _, base := range baseCandidates {
	if tileTime(base).Before(tileTime(newer)) {
	    candidates = append(candidates, base)
	}
    }
    if len(candidates) == 0 {
	// Нет строго ранних — берём всех (периоды заданы некорректно)
	candidates = baseCandidates
    }
    // Самый свежий из кандидатов (как в kosmo: sort desc → [0])
    picked := candidates[0]
    for _, base := range candidates[1:] {
	if tileTime(base).After(tileTime(picked)) {
	    picked = base
	}
    }
    return picked, true
}

func rootForSpec(app *AppConfig, spec ModelSpec) string {
    if normalizePreprocess(spec.Preprocess) == "sentinel2a" && strings.TrimSpace(app.Sentinel2A) != "" {
	return app.Sentinel2A
    }
    return app.Sentinel
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
    // Строим маску валидных пикселей для всех режимов.
    validMask := combineValidMasks(cubes)

    if len(img) == 0 || len(img[0]) == 0 || len(img[0][0]) == 0 || len(img[0][0][0]) == 0 {
	return "", errors.New("empty input image")
    }
    h, w := len(img[0][0]), len(img[0][0][0])

    // Определяем шаг тайла.
    // Для многоклассовых моделей используется overlap (как в kosmo forest_disease_v3),
    // для остальных — bound.
    tileH, tileW := spec.Tile, spec.Tile
    var stepY, stepX int
    if spec.IsMulticlass() && spec.Overlap > 0 {
	stepY = tileH - spec.Overlap
	stepX = tileW - spec.Overlap
    } else {
	bound := spec.Bound
	stepY = tileH - 2*bound
	stepX = tileW - 2*bound
    }
    if stepY <= 0 || stepX <= 0 {
	return "", fmt.Errorf("invalid step: tile=%d bound=%d overlap=%d", tileH, spec.Bound, spec.Overlap)
    }
    inChannels := len(img)

    // Для многоклассовой модели накапливаем вероятности по всем классам.
    numClasses := spec.NumClasses
    if numClasses < 1 {
	numClasses = 1
    }
    // out: для бинарных — float32 маска [h*w], для многоклассовых — накопленные
    // вероятности [numClasses*h*w] и счётчики [h*w].
    out := make([]float32, h*w)
    var multiOut []float32   // накопленные logits/probs [numClasses*h*w]
    var multiCount []float32 // счётчик вкладов каждого пикселя [h*w]
    if spec.IsMulticlass() {
	multiOut = make([]float32, numClasses*h*w)
	multiCount = make([]float32, h*w)
    }

    type tileMeta struct{ dstY0, dstY1, dstX0, dstX1, srcY0, srcX0, realH, realW int }
    var metas []tileMeta
    var batchInput []float32
    var batchValid []uint8

    flush := func() error {
	if len(metas) == 0 {
	    return nil
	}
	preds, err := sess.Predict(batchInput, len(metas), inChannels, tileH, tileW, numClasses)
	if err != nil {
	    return err
	}
	pixelCount := tileH * tileW
	for bi, m := range metas {
	    if spec.IsMulticlass() {
		// Многоклассовый: накапливаем predprobs для реальной области патча.
		// Python: pred_patch_cropped = pred_patch_probs[:, :img_patch.shape[1], :img_patch.shape[2]]
		//         prediction[:, y:y_end, x:x_end] += pred_patch_cropped
		//         count[y:y_end, x:x_end] += 1.0
		// Паддинговая зона (за realH/realW) и nodata-пиксели не суммируются.
		for yy := m.dstY0; yy < m.dstY1; yy++ {
		    py := (yy - m.dstY0) + m.srcY0
		    if py >= m.realH {
			break
		    }
		    for xx := m.dstX0; xx < m.dstX1; xx++ {
			px := (xx - m.dstX0) + m.srcX0
			if px >= m.realW {
			    break
			}
			pOff := py*tileW + px
			if batchValid[bi*pixelCount+pOff] == 0 {
			    continue
			}
			pixOut := yy*w + xx
			multiCount[pixOut] += 1.0
			for cl := 0; cl < numClasses; cl++ {
			    predIdx := bi*numClasses*pixelCount + cl*pixelCount + pOff
			    multiOut[cl*h*w+pixOut] += preds[predIdx]
			}
		    }
		}
	    } else {
		// Бинарный: threshold
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
	}
	metas, batchInput, batchValid = metas[:0], batchInput[:0], batchValid[:0]
	return nil
    }

    // channel_means: используются для заполнения nodata/padding пикселей
    // (как interpolate_patch в Python). НЕ вычитаются из данных —
    // модель обучена на TOA-нормированных данных без вычитания средних.
    channelMeans := make([]float32, inChannels)
    if len(spec.ChannelMeans) == inChannels {
	for i, m := range spec.ChannelMeans {
	    channelMeans[i] = float32(m)
	}
    }
    hasChannelMeans := len(spec.ChannelMeans) == inChannels

    bound := spec.Bound

    for y0 := 0; y0 < h; y0 += stepY {
	for x0 := 0; x0 < w; x0 += stepX {
	    patch := make([]float32, inChannels*tileH*tileW)
	    valid := make([]uint8, tileH*tileW)
	    // realH/realW — реальный размер патча (без паддинга за краями)
	    realH := tileH
	    if y0+tileH > h {
		realH = h - y0
	    }
	    realW := tileW
	    if x0+tileW > w {
		realW = w - x0
	    }
	    for yy := 0; yy < tileH; yy++ {
		sy := y0 + yy
		if sy >= h {
		    // Паддинг за краем: заполняем channel_means (как Python nan_to_num(nan=1) → ~0.5)
		    if hasChannelMeans {
			for xx := 0; xx < tileW; xx++ {
			    for c := 0; c < inChannels; c++ {
				patch[c*tileH*tileW+yy*tileW+xx] = channelMeans[c]
			    }
			}
		    }
		    continue
		}
		for xx := 0; xx < tileW; xx++ {
		    sx := x0 + xx
		    if sx >= w {
			// Паддинг за краем по X
			if hasChannelMeans {
			    for c := 0; c < inChannels; c++ {
				patch[c*tileH*tileW+yy*tileW+xx] = channelMeans[c]
			    }
			}
			continue
		    }
		    pixIdx := sy*w + sx
		    isNodata := len(validMask) > 0 && validMask[pixIdx] == 0
		    if isNodata {
			// Nodata внутри изображения: заполняем channel_means чтобы
			// модель не получила нули/NaN в паддинге.
			// valid=0 для всех режимов — nodata-пиксели не учитываются
			// в postMask (бинарный) и не накапливаются в multiCount (multiclass).
			if hasChannelMeans {
			    for c := 0; c < inChannels; c++ {
				patch[c*tileH*tileW+yy*tileW+xx] = channelMeans[c]
			    }
			}
			continue
		    }
		    for c := 0; c < inChannels; c++ {
			val := preprocessBySpec(img[c][0][sy][sx], spec)
			patch[c*tileH*tileW+yy*tileW+xx] = val
		    }
		    valid[yy*tileW+xx] = 1
		}
	    }
	    // Для многоклассовых (overlap): записываем весь тайл целиком,
	    // flush накапливает с count → итоговый argmax по среднему.
	    // Для бинарных (bound): отбрасываем граничную полосу.
	    var srcY0, srcY1, srcX0, srcX1 int
	    if spec.IsMulticlass() && spec.Overlap > 0 {
		srcY0, srcY1 = 0, min(tileH, h-y0)
		srcX0, srcX1 = 0, min(tileW, w-x0)
	    } else {
		srcY0, srcY1 = bound, tileH-bound
		srcX0, srcX1 = bound, tileW-bound
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
	    }
	    dstY0, dstY1 := y0+srcY0, min(y0+srcY1, h)
	    dstX0, dstX1 := x0+srcX0, min(x0+srcX1, w)

	    metas = append(metas, tileMeta{dstY0: dstY0, dstY1: dstY1, dstX0: dstX0, dstX1: dstX1, srcY0: srcY0, srcX0: srcX0, realH: realH, realW: realW})
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
    postMask := combineValidMasks(cubes)

    if spec.IsMulticlass() {
	// Строим множество классов-детекций из конфига.
	// Если detection_classes не задан — детекция = любой класс > 0 (умолчание).
	detectionSet := make(map[int]bool, len(spec.DetectionClasses))
	for _, cl := range spec.DetectionClasses {
	    detectionSet[cl] = true
	}
	useDetectionSet := len(detectionSet) > 0

	for i := 0; i < h*w; i++ {
	    if multiCount[i] == 0 {
		out[i] = 0
		continue
	    }
	    if len(postMask) > 0 && postMask[i] == 0 {
		out[i] = 0
		continue
	    }
	    // Argmax — класс с максимальным logit
	    bestCl := 0
	    bestVal := multiOut[0*h*w+i] / multiCount[i]
	    for cl := 1; cl < numClasses; cl++ {
		v := multiOut[cl*h*w+i] / multiCount[i]
		if v > bestVal {
		    bestVal = v
		    bestCl = cl
		}
	    }
	    _ = bestVal
	    var isDetection bool
	    if useDetectionSet {
		isDetection = detectionSet[bestCl]
	    } else {
		isDetection = bestCl > 0
	    }
	    if isDetection {
		out[i] = float32(bestCl)
	    } else {
		out[i] = 0
	    }
	}
    } else {
	// Бинарный: финальная маскировка nodata
	if len(postMask) == len(out) {
	    for i := range out {
		if postMask[i] == 0 {
		    out[i] = 0
		}
	    }
	}
    }

    if spec.MaskFilter.Enabled {
	log.Printf("mask filter start: %s", spec.Name)
	out = applyMaskFilter(out, w, h, spec.MaskFilter)
	log.Printf("mask filter done: %s", spec.Name)
    }
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
	if err != nil {
	    return err
	}
	out, err := os.Create(dst)
	if err != nil {
	    in.Close()
	    return err
	}
	if _, err := io.Copy(out, in); err != nil {
	    out.Close()
	    in.Close()
	    return err
	}
	out.Close()
	in.Close()
    }
    return nil
}