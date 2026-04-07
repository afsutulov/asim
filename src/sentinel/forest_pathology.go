package main

// forest_pathology.go — статистический алгоритм детекции усыхания леса.
//
// Точно воспроизводит kosmo SemanticSegmentationInferenceModelForestPathology:
//   1. Читает один летний снимок, вычисляет VOG1 = B05/B06.
//   2. Строит гистограмму VOG1 по валидным пикселям (100 бинов).
//   3. Находит моду через анализ пиков (find_peaks + prominence + argrelmax).
//   4. threshold = mode − 0.75 × (mode − left_edge).
//   5. mask = (VOG1 < threshold) → больной лес.
//   6. Полигонизирует маску.
//
// Нейронная сеть не используется. model_path = None в kosmo.

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
)

// runForestPathologyPeriod — главная точка входа для preprocess="pathology".
// Берёт один летний период, для каждого tile_id выбирает лучший снимок
// (min cloud), вычисляет VOG1 и применяет статистический порог.
func runForestPathologyPeriod(cfg ProcessConfig, allowedGeom Geometry, workDir string) (periodRunResult, error) {
	tiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, cfg.Start, cfg.End, cfg.Cloud)
	if err != nil {
		return periodRunResult{}, err
	}
	stats := ProcessStats{PrimaryCandidates: len(tiles)}
	if len(tiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no tiles in period %s..%s", cfg.Start, cfg.End)
	}

	allowedEnv := GeometryEnvelope(allowedGeom)
	filtered, skipped := collectAllowedTiles(tiles, allowedGeom, allowedEnv)
	stats.TileFootprintsSeen = len(tiles)
	stats.TilesSkippedOutside = skipped

	// Группируем по tile_id, выбираем лучший снимок (min cloud).
	byTile := make(map[string]SafeTile)
	for _, t := range filtered {
		id := strings.TrimSpace(t.TileID)
		if id == "" {
			id = tileIDFromImgDataPath(t.ImgDataPath)
		}
		cur, ok := byTile[id]
		if !ok || t.Cloud < cur.Cloud {
			byTile[id] = t
		}
	}

	ids := make([]string, 0, len(byTile))
	for id := range byTile {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	log.Printf("forest_pathology: %d tile_ids selected", len(ids))

	mergedShp := filepath.Join(workDir, "forest_pathology_merged.shp")
	_ = os.Remove(mergedShp)
	// Параллельная обработка тайлов: тайлы независимы, bottleneck — NFS.
	// workers = min(GOMAXPROCS, 4) чтобы не перегрузить NFS.
	workers := runtime.GOMAXPROCS(0)
	if workers > 4 {
		workers = 4
	}
	progress := newProgressLogger(len(ids))

	type result struct {
		id  string
		shp string
		err error
	}
	jobs := make(chan string, len(ids))
	results := make(chan result, len(ids))
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := range jobs {
				tile := byTile[id]
				shp, err := runForestPathologyOnTile(tile, allowedGeom, workDir, cfg.MinArea, cfg.Simplify)
				results <- result{id: id, shp: shp, err: err}
			}
		}()
	}
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	go func() { wg.Wait(); close(results) }()

	done := 0
	for r := range results {
		done++
		if r.err != nil {
			stats.TilesReadErrors++
			log.Printf("forest_pathology: error %s: %v", r.id, r.err)
		} else {
			stats.TilesProcessed++
			stats.ModelRuns++
			if err := AppendShapefileFeaturesClipped(r.shp, allowedGeom, mergedShp); err != nil {
				log.Printf("forest_pathology: merge error %s: %v", r.id, err)
			}
			CleanupShapefileSet(r.shp)
		}
		progress.Update(done)
	}

	if _, err := os.Stat(mergedShp); err != nil {
		return periodRunResult{stats: stats}, fmt.Errorf("no polygons found")
	}

	// Dissolve не нужен: Sentinel тайлы не перекрываются пространственно.
	// DissolveOverlappingPolygons при 100k+ полигонах работает часами (O(N²)).
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
}

// runForestPathologyOnTile вычисляет VOG1, находит порог, полигонизирует.
func runForestPathologyOnTile(tile SafeTile, allowedGeom Geometry, workDir string, minArea, simplify float64) (string, error) {
	// Читаем B05 и B06 (20m разрешение).
	brB05, err := readSingleBand(bandFile(tile, "B05"))
	if err != nil {
		return "", fmt.Errorf("read B05: %w", err)
	}
	brB06, err := readSingleBand(bandFile(tile, "B06"))
	if err != nil {
		return "", fmt.Errorf("read B06: %w", err)
	}

	// Если разрешения отличаются — ресэмплируем B06 к B05.
	b05 := brB05.data
	b06 := brB06.data
	if brB05.w != brB06.w || brB05.h != brB06.h {
		b06 = bilinearResample(brB06, brB05)
	}
	w, h := brB05.w, brB05.h
	n := w * h

	// TOA нормировка.
	for i := range b05 {
		b05[i] = sentinelTOAValue(b05[i], tile.Date)
	}
	for i := range b06 {
		b06[i] = sentinelTOAValue(b06[i], tile.Date)
	}

	// Простая облачная маска B10/B11/B12.
	brB10, err := readSingleBand(bandFile(tile, "B10"))
	if err != nil {
		return "", fmt.Errorf("read B10: %w", err)
	}
	brB11, err := readSingleBand(bandFile(tile, "B11"))
	if err != nil {
		return "", fmt.Errorf("read B11: %w", err)
	}
	brB12, err := readSingleBand(bandFile(tile, "B12"))
	if err != nil {
		return "", fmt.Errorf("read B12: %w", err)
	}
	b10 := bilinearResample(brB10, brB05)
	b11 := bilinearResample(brB11, brB05)
	b12 := bilinearResample(brB12, brB05)
	for i := range b10 {
		b10[i] = sentinelTOAValue(b10[i], tile.Date)
		b11[i] = sentinelTOAValue(b11[i], tile.Date)
		b12[i] = sentinelTOAValue(b12[i], tile.Date)
	}
	cloudMask := buildCloudMask(map[string][]float32{
		"B10": b10, "B11": b11, "B12": b12,
	}, n)

	// VOG1 = B05/B06 с облачной маской.
	vog1 := make([]float32, n)
	validCount := 0
	for i := 0; i < n; i++ {
		if cloudMask[i] != 0 || b05[i] <= 0 || b06[i] <= 0 ||
			b05[i] == sentinelNoData || b06[i] == sentinelNoData {
			vog1[i] = float32(math.NaN())
			continue
		}
		v := b05[i] / b06[i]
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			vog1[i] = float32(math.NaN())
			continue
		}
		vog1[i] = v
		validCount++
	}

	if validCount < 100 {
		return "", fmt.Errorf("too few valid VOG1 pixels: %d", validCount)
	}

	// Статистический порог — точно как в kosmo __call__:
	threshold := computePathologyThreshold(vog1)
	// Ограничение threshold: если алгоритм нашёл аномальный порог
	// (например снег/лёд смещает гистограмму), пропускаем тайл.
	// Нормальный диапазон VOG1 здорового леса: 0.8-1.1, threshold < 0.95.
	// threshold=-1 означает аномальный снимок (p75 защита сработала).
	if threshold < 0 {
		log.Printf("forest_pathology: tile=%s SKIP — anomalous threshold (histogram contaminated)",
			tile.TileID)
		return "", fmt.Errorf("anomalous threshold: histogram contaminated by non-forest pixels")
	}
	log.Printf("forest_pathology: tile=%s vog1_threshold=%.4f valid_px=%d",
		tile.TileID, threshold, validCount)

	// Маска как в kosmo: VOG1 < threshold.
	// NaN (облака) → NaN → порог+9999 в kosmo, у нас просто 0 (не маркируем).
	mask := make([]float32, n)
	for i := 0; i < n; i++ {
		if !math.IsNaN(float64(vog1[i])) && vog1[i] < threshold {
			mask[i] = 1
		}
	}

	// Записываем маску в GeoTIFF.
	maskTif := filepath.Join(workDir, fmt.Sprintf("fp_%s_mask.tif", tile.TileID))
	if err := WriteGeoTIFF1(maskTif, mask, w, h, brB05.geo, brB05.proj); err != nil {
		return "", fmt.Errorf("write mask tif: %w", err)
	}

	// Полигонизация.
	shp, err := PolygonizeMask(maskTif, 1, minArea, simplify)
	if err != nil {
		_ = os.Remove(maskTif)
		return "", fmt.Errorf("polygonize: %w", err)
	}
	_ = os.Remove(maskTif)
	return shp, nil
}

// computeForestPathologyMaskForTile вычисляет бинарную маску болезни в НАТИВНОЙ сетке снимка.
// Это должен быть тот же результат, что использует forest_disease для polygonize.
func computeForestPathologyMaskForTile(tile SafeTile) ([]float32, *bandRaster, float32, int, error) {
	vog1, ref, w, h, _, _, err := readVOG1ForTile(tile)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	threshold := computePathologyThreshold(vog1)
	if threshold < 0 {
		return nil, nil, threshold, 0, fmt.Errorf("anomalous threshold")
	}
	n := w * h
	mask := make([]float32, n)
	validCount := 0
	for i := 0; i < n; i++ {
		if !math.IsNaN(float64(vog1[i])) {
			validCount++
			if vog1[i] < threshold {
				mask[i] = 1
			}
		}
	}
	return mask, ref, threshold, validCount, nil
}

// nearestResampleGeo приводит бинарную/категориальную маску src к сетке ref по геопривязке.
// В отличие от bilinearResample учитывает geo transform, поэтому подходит для diff по двум датам.
func nearestResampleGeo(src *bandRaster, ref *bandRaster) []float32 {
	out := make([]float32, ref.w*ref.h)
	sx0, sy0 := src.geo[0], src.geo[3]
	spx, spy := src.geo[1], src.geo[5]
	rx0, ry0 := ref.geo[0], ref.geo[3]
	rpx, rpy := ref.geo[1], ref.geo[5]
	for y := 0; y < ref.h; y++ {
		wy := ry0 + (float64(y)+0.5)*rpy
		syf := (wy-sy0)/spy - 0.5
		sy := int(math.Round(syf))
		if sy < 0 || sy >= src.h {
			continue
		}
		for x := 0; x < ref.w; x++ {
			wx := rx0 + (float64(x)+0.5)*rpx
			sxf := (wx-sx0)/spx - 0.5
			sx := int(math.Round(sxf))
			if sx < 0 || sx >= src.w {
				continue
			}
			out[y*ref.w+x] = src.data[sy*src.w+sx]
		}
	}
	return out
}

// computePathologyThreshold точно воспроизводит kosmo:
// - гистограмма 100 бинов по всем валидным пикселям (как в kosmo)
// - find_peaks → prominence фильтрация → argrelmax(order=15) → мода
// - threshold = mode − 0.75 × (mode − left_edge)
// - защита: если threshold > p75(лесных пикселей [0.3,1.5]) → возвращает -1 (скип тайла)
func computePathologyThreshold(vog1 []float32) float32 {
	const (
		numBins             = 100
		minHistValue        = 0.1
		widthThreshold      = 0.75
		prominenceThreshold = 10.0
		argrelOrder         = 15
	)

	// Kosmo: гистограмма по ВСЕМ не-NaN пикселям
	vals := make([]float64, 0, len(vog1))
	forestVals := make([]float64, 0, len(vog1)) // для защиты p75
	for _, v := range vog1 {
		if !math.IsNaN(float64(v)) && !math.IsInf(float64(v), 0) {
			vals = append(vals, float64(v))
			if float64(v) >= 0.3 && float64(v) <= 1.5 {
				forestVals = append(forestVals, float64(v))
			}
		}
	}
	if len(vals) == 0 {
		return 0.5
	}

	minV, maxV := vals[0], vals[0]
	for _, v := range vals {
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	if maxV == minV {
		return float32(minV)
	}

	binWidth := (maxV - minV) / float64(numBins)
	hist := make([]float64, numBins)
	bins := make([]float64, numBins)
	for b := range bins {
		bins[b] = minV + float64(b)*binWidth
	}
	for _, v := range vals {
		b := int((v - minV) / binWidth)
		if b >= numBins {
			b = numBins - 1
		}
		hist[b]++
	}
	total := float64(len(vals)) * binWidth
	for b := range hist {
		hist[b] /= total
	}

	modeEstim := bins[argmaxFloat(hist)]

	peaks := findPeaks(hist)
	histMod := make([]float64, len(hist))
	copy(histMod, hist)
	if len(peaks) > 0 {
		prominences := peakProminences(hist, peaks, 9)
		for i, pk := range peaks {
			if prominences[i] > prominenceThreshold {
				histMod[pk] = hist[pk] - prominences[i]
				if histMod[pk] < 0 {
					histMod[pk] = 0
				}
			}
		}
	}

	modeCorrected := modeEstim
	bestPeaks := argRelMax(histMod, argrelOrder)
	if len(bestPeaks) > 0 {
		bestIdx := 0
		for i := 1; i < len(bestPeaks); i++ {
			if histMod[bestPeaks[i]] > histMod[bestPeaks[bestIdx]] {
				bestIdx = i
			}
		}
		modeCorrected = bins[bestPeaks[bestIdx]]
	}

	stdIndex := 0
	for i, v := range hist {
		if v > minHistValue {
			stdIndex = i
			break
		}
	}
	width := modeCorrected - bins[stdIndex]
	threshold := modeCorrected - widthThreshold*width

	// Защита от заливки нелесными объектами:
	// если threshold > p75 лесных пикселей → алгоритм нашёл неверный пик → скип тайла.
	// Возвращаем -1 как сигнал скипа.
	if len(forestVals) > 0 {
		sort.Float64s(forestVals)
		p75 := forestVals[len(forestVals)*75/100]
		if float64(threshold) > p75 {
			return -1 // сигнал аномалии
		}
	}

	return float32(threshold)
}

// findPeaks — scipy.signal.find_peaks: индексы локальных максимумов.
func findPeaks(x []float64) []int {
	var peaks []int
	for i := 1; i < len(x)-1; i++ {
		if x[i] > x[i-1] && x[i] > x[i+1] {
			peaks = append(peaks, i)
		}
	}
	return peaks
}

// peakProminences — scipy.signal.peak_prominences с wlen=9.
// Prominence = высота пика минус максимальное значение в окне wlen по обе стороны.
func peakProminences(x []float64, peaks []int, wlen int) []float64 {
	proms := make([]float64, len(peaks))
	half := wlen / 2
	for i, pk := range peaks {
		left := pk - half
		if left < 0 {
			left = 0
		}
		right := pk + half
		if right >= len(x) {
			right = len(x) - 1
		}
		// Минимальное значение в окне (base).
		base := x[left]
		for j := left; j <= right; j++ {
			if x[j] < base {
				base = x[j]
			}
		}
		proms[i] = x[pk] - base
	}
	return proms
}

// argRelMax — scipy.signal.argrelmax(order=N): индексы где x[i] > x[i±1..N].
func argRelMax(x []float64, order int) []int {
	var result []int
	for i := order; i < len(x)-order; i++ {
		isMax := true
		for d := 1; d <= order; d++ {
			if x[i] <= x[i-d] || x[i] <= x[i+d] {
				isMax = false
				break
			}
		}
		if isMax {
			result = append(result, i)
		}
	}
	return result
}

func argmaxFloat(x []float64) int {
	best := 0
	for i := 1; i < len(x); i++ {
		if x[i] > x[best] {
			best = i
		}
	}
	return best
}

// ── Модель "сравнение год к году" (preprocess="pathology_diff") ───────────
//
// Логика:
//  1. Прогоняем статистический алгоритм VOG1 на новом периоде (Start..End).
//  2. Прогоняем тот же алгоритм на базовом периоде (Start2..End2).
//  3. Результат = пиксели больные в новом И здоровые в базовом.
//     Т.е. только новые очаги болезни, которых не было раньше.

// runForestPathologyDiffPeriod — "усыхание год к году".
func runForestPathologyDiffPeriod(cfg ProcessConfig, allowedGeom Geometry, workDir string) (periodRunResult, error) {
	// Загружаем оба периода.
	newTiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, cfg.Start, cfg.End, cfg.Cloud)
	if err != nil {
		return periodRunResult{}, err
	}
	baseTiles, err := loadTilesForCfg(cfg.CashPath, cfg.App.Sentinel, cfg.Start2, cfg.End2, cfg.Cloud)
	if err != nil {
		return periodRunResult{}, err
	}

	stats := ProcessStats{
		PrimaryCandidates:   len(newTiles),
		SecondaryCandidates: len(baseTiles),
	}
	if len(newTiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no tiles in new period %s..%s", cfg.Start, cfg.End)
	}
	if len(baseTiles) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no tiles in base period %s..%s", cfg.Start2, cfg.End2)
	}

	allowedEnv := GeometryEnvelope(allowedGeom)
	filteredNew, skNew := collectAllowedTiles(newTiles, allowedGeom, allowedEnv)
	filteredBase, skBase := collectAllowedTiles(baseTiles, allowedGeom, allowedEnv)
	stats.TileFootprintsSeen = len(newTiles) + len(baseTiles)
	stats.TilesSkippedOutside = skNew + skBase

	// Группируем по tile_id — лучший снимок (min cloud) из каждого периода.
	bestNew := bestPerTile(filteredNew)
	bestBase := bestPerTile(filteredBase)

	// Обрабатываем только tile_id которые есть в обоих периодах.
	ids := make([]string, 0)
	for id := range bestNew {
		if _, ok := bestBase[id]; ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) == 0 {
		return periodRunResult{stats: stats}, fmt.Errorf("no common tile_ids in both periods")
	}

	log.Printf("forest_pathology_diff: %d common tile_ids", len(ids))

	mergedShp := filepath.Join(workDir, "fp_diff_merged.shp")
	_ = os.Remove(mergedShp)

	// Параллельная обработка: JP2-чтение и VOG1-вычисление не зависят от GDAL/OGR.
	// AppendShapefileFeaturesClipped пишет в один файл — только из main goroutine.
	progress := newProgressLogger(len(ids))
	// GDAL JP2-чтение не потокобезопасно при параллельном использовании.
	// forest_disease_diff выполняется последовательно.
	for done, id := range ids {
		tNew := bestNew[id]
		tBase := bestBase[id]
		shp, err := runForestPathologyDiffOnTile(tNew, tBase, workDir, cfg.MinArea, cfg.Simplify)
		if err != nil {
			stats.TilesReadErrors++
			log.Printf("forest_pathology_diff: error %s: %v", id, err)
		} else {
			stats.TilesProcessed++
			stats.ModelRuns++
			if err := AppendShapefileFeaturesClipped(shp, allowedGeom, mergedShp); err != nil {
				log.Printf("forest_pathology_diff: merge error %s: %v", id, err)
			}
			CleanupShapefileSet(shp)
		}
		progress.Update(done + 1)
	}

	if _, err := os.Stat(mergedShp); err != nil {
		return periodRunResult{stats: stats}, fmt.Errorf("no new disease polygons found")
	}

	// Dissolve не нужен: тайлы не перекрываются.
	return periodRunResult{shpPath: mergedShp, stats: stats}, nil
}

// runForestPathologyDiffOnTile находит НОВЫЕ очаги усыхания:
// полигоны больные в new-периоде, которых не было в base-периоде.
//
// Подход: полигонизируем каждый период отдельно, потом геометрически
// попиксельный diff: быстро, результат = новые очаги которых не было в базовом году.
// runForestPathologyDiffOnTile полигонизирует оба периода с ЕДИНЫМ порогом.
// Единый порог — из нового снимка — гарантирует что базовые и новые полигоны
// сопоставимы: базовые не поглощают новые из-за более высокого порога.
func runForestPathologyDiffOnTile(tileNew, tileBase SafeTile, workDir string, minArea, simplify float64) (string, error) {
	// Читаем VOG1 нового периода.
	vog1New, _, newW, newH, newGeo, newProj, err := readVOG1ForTile(tileNew)
	if err != nil {
		return "", fmt.Errorf("new period read: %w", err)
	}
	threshNew := computePathologyThreshold(vog1New)
	if threshNew < 0 {
		return "", fmt.Errorf("new period: anomalous threshold for tile %s", tileNew.TileID)
	}

	// Читаем VOG1 базового периода и его порог.
	vog1Base, baseRef, _, _, _, _, err := readVOG1ForTile(tileBase)
	if err != nil {
		return "", fmt.Errorf("base period read: %w", err)
	}
	threshBase := computePathologyThreshold(vog1Base)
	// При аномальном базовом пороге — вычитать нечего, base считается пустым.
	if threshBase < 0 {
		threshBase = threshNew
	}

	// Ресэмплируем VOG1_base в сетку нового снимка (float, не маску).
	vog1BaseR := nearestResampleGeo(&bandRaster{
		data: vog1Base, geo: baseRef.geo, proj: baseRef.proj,
		w: baseRef.w, h: baseRef.h,
	}, &bandRaster{geo: newGeo, proj: newProj, w: newW, h: newH})

	log.Printf("forest_pathology_diff: tile=%s thresh_new=%.4f thresh_base=%.4f",
		tileNew.TileID, threshNew, threshBase)

	// Попиксельный diff с РАЗДЕЛЬНЫМИ порогами каждого года:
	// Новый очаг = больной в 2025 по меркам 2025 И здоровый в 2024 по меркам 2024.
	// Это эквивалентно: forest_disease_2025 MINUS forest_disease_2024.
	n := newW * newH
	mask := make([]float32, n)
	for i := 0; i < n; i++ {
		isSick := !math.IsNaN(float64(vog1New[i])) && vog1New[i] < threshNew
		wasSick := !math.IsNaN(float64(vog1BaseR[i])) && vog1BaseR[i] < threshBase
		if isSick && !wasSick {
			mask[i] = 1
		}
	}

	maskTif := filepath.Join(workDir, fmt.Sprintf("fpdiff_%s_mask.tif", tileNew.TileID))
	defer os.Remove(maskTif)
	if err := WriteGeoTIFF1(maskTif, mask, newW, newH, newGeo, newProj); err != nil {
		return "", fmt.Errorf("write mask: %w", err)
	}
	shp, err := PolygonizeMask(maskTif, 1, minArea, simplify)
	if err != nil {
		return "", fmt.Errorf("polygonize: %w", err)
	}
	return shp, nil
}

// polygonizeWithThreshold строит маску VOG1 < threshold и полигонизирует.
// vog1, geo, proj — уже прочитанные данные (без повторного чтения JP2).
func polygonizeWithThreshold(tile SafeTile, vog1 []float32, w, h int, geo [6]float64, proj string, threshold float32, workDir string, minArea, simplify float64) (string, error) {
	n := w * h
	mask := make([]float32, n)
	for i := 0; i < n; i++ {
		if !math.IsNaN(float64(vog1[i])) && vog1[i] < threshold {
			mask[i] = 1
		}
	}
	maskTif := filepath.Join(workDir, fmt.Sprintf("fpdiff_%s_%s_mask.tif", tile.TileID, tile.Date))
	defer os.Remove(maskTif)
	if err := WriteGeoTIFF1(maskTif, mask, w, h, geo, proj); err != nil {
		return "", fmt.Errorf("write mask: %w", err)
	}
	shp, err := PolygonizeMask(maskTif, 1, minArea, simplify)
	if err != nil {
		return "", fmt.Errorf("polygonize: %w", err)
	}
	return shp, nil
}

// polygonizePathologyTile вычисляет VOG1 маску и полигонизирует тайл.
// В отличие от runForestPathologyOnTile не принимает allowedGeom —
// обрезка по AOI делается позже в AppendShapefileFeaturesClipped.
func polygonizePathologyTile(tile SafeTile, workDir string, minArea, simplify float64) (string, error) {
	vog1, brB05, w, h, geo, proj, err := readVOG1ForTile(tile)
	if err != nil {
		return "", err
	}
	threshold := computePathologyThreshold(vog1)
	if threshold < 0 {
		return "", fmt.Errorf("anomalous threshold %.4f for tile %s", threshold, tile.TileID)
	}
	_ = brB05
	n := w * h
	mask := make([]float32, n)
	for i := 0; i < n; i++ {
		if !math.IsNaN(float64(vog1[i])) && vog1[i] < threshold {
			mask[i] = 1
		}
	}
	maskTif := filepath.Join(workDir, fmt.Sprintf("fpdiff_%s_%s_mask.tif", tile.TileID, tile.Date))
	defer os.Remove(maskTif)
	if err := WriteGeoTIFF1(maskTif, mask, w, h, geo, proj); err != nil {
		return "", fmt.Errorf("write mask: %w", err)
	}
	shp, err := PolygonizeMask(maskTif, 1, minArea, simplify)
	if err != nil {
		return "", fmt.Errorf("polygonize: %w", err)
	}
	return shp, nil
}

// readVOG1ForTile читает B05/B06 и возвращает VOG1 с облачной маской.
// Возвращает также опорный raster B05, чтобы другой период можно было
// привести к точно той же сетке перед попиксельным diff.
func readVOG1ForTile(tile SafeTile) ([]float32, *bandRaster, int, int, [6]float64, string, error) {
	brB05, err := readSingleBand(bandFile(tile, "B05"))
	if err != nil {
		return nil, nil, 0, 0, [6]float64{}, "", fmt.Errorf("B05: %w", err)
	}
	vog1, w, h, geo, proj, err := readVOG1ForTileOnRef(tile, brB05)
	if err != nil {
		return nil, nil, 0, 0, [6]float64{}, "", err
	}
	return vog1, brB05, w, h, geo, proj, nil
}

// readVOG1ForTileOnRef читает tile и приводит все каналы к заданной сетке ref.
// Это критично для forest_disease_diff: новый и базовый периоды нужно сравнивать
// в одной и той же пиксельной сетке, иначе старые полигоны массово попадают в diff
// из-за субпиксельного сдвига между сценами разных дат.
func readVOG1ForTileOnRef(tile SafeTile, ref *bandRaster) ([]float32, int, int, [6]float64, string, error) {
	brB05, err := readSingleBand(bandFile(tile, "B05"))
	if err != nil {
		return nil, 0, 0, [6]float64{}, "", fmt.Errorf("B05: %w", err)
	}
	brB06, err := readSingleBand(bandFile(tile, "B06"))
	if err != nil {
		return nil, 0, 0, [6]float64{}, "", fmt.Errorf("B06: %w", err)
	}
	brB10, err := readSingleBand(bandFile(tile, "B10"))
	if err != nil {
		return nil, 0, 0, [6]float64{}, "", fmt.Errorf("B10: %w", err)
	}
	brB11, err := readSingleBand(bandFile(tile, "B11"))
	if err != nil {
		return nil, 0, 0, [6]float64{}, "", fmt.Errorf("B11: %w", err)
	}
	brB12, err := readSingleBand(bandFile(tile, "B12"))
	if err != nil {
		return nil, 0, 0, [6]float64{}, "", fmt.Errorf("B12: %w", err)
	}

	// Все каналы, включая B05, приводим к сетке ref.
	b05 := bilinearResample(brB05, ref)
	b06 := bilinearResample(brB06, ref)
	b10 := bilinearResample(brB10, ref)
	b11 := bilinearResample(brB11, ref)
	b12 := bilinearResample(brB12, ref)

	w, h := ref.w, ref.h
	n := w * h

	for i := 0; i < n; i++ {
		b05[i] = sentinelTOAValue(b05[i], tile.Date)
		b06[i] = sentinelTOAValue(b06[i], tile.Date)
		b10[i] = sentinelTOAValue(b10[i], tile.Date)
		b11[i] = sentinelTOAValue(b11[i], tile.Date)
		b12[i] = sentinelTOAValue(b12[i], tile.Date)
	}

	cloudMask := buildCloudMask(map[string][]float32{
		"B10": b10, "B11": b11, "B12": b12,
	}, n)

	vog1 := make([]float32, n)
	for i := 0; i < n; i++ {
		if cloudMask[i] != 0 || b05[i] <= 0 || b06[i] <= 0 ||
			b05[i] == sentinelNoData || b06[i] == sentinelNoData {
			vog1[i] = float32(math.NaN())
			continue
		}
		v := b05[i] / b06[i]
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			vog1[i] = float32(math.NaN())
			continue
		}
		vog1[i] = v
	}

	return vog1, w, h, ref.geo, ref.proj, nil
}

// dilateBinaryMask расширяет бинарную маску на radius пикселей (8-связность).
func dilateBinaryMask(mask []float32, w, h, radius int) []float32 {
	if radius <= 0 {
		out := make([]float32, len(mask))
		copy(out, mask)
		return out
	}
	cur := make([]float32, len(mask))
	copy(cur, mask)
	for step := 0; step < radius; step++ {
		next := make([]float32, len(cur))
		copy(next, cur)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				if cur[y*w+x] <= 0.5 {
					continue
				}
				for dy := -1; dy <= 1; dy++ {
					ny := y + dy
					if ny < 0 || ny >= h {
						continue
					}
					for dx := -1; dx <= 1; dx++ {
						nx := x + dx
						if nx < 0 || nx >= w {
							continue
						}
						next[ny*w+nx] = 1
					}
				}
			}
		}
		cur = next
	}
	return cur
}

// subtractOldComponents удаляет из newMask целиком те связные компоненты,
// которые существенно пересекаются со старой маской. Это лучше соответствует
// задаче "оставить только действительно новые полигоны", чем попиксельный diff,
// который оставляет старые очаги как расширившиеся кольца.
func subtractOldComponents(newMask, oldMask []float32, w, h int, overlapRatio float64) []float32 {
	out := make([]float32, len(newMask))
	visited := make([]uint8, len(newMask))
	queue := make([]int, 0, 1024)
	component := make([]int, 0, 1024)
	for i := range newMask {
		if visited[i] != 0 || newMask[i] <= 0.5 {
			continue
		}
		visited[i] = 1
		queue = queue[:0]
		component = component[:0]
		queue = append(queue, i)
		overlap := 0
		for len(queue) > 0 {
			idx := queue[len(queue)-1]
			queue = queue[:len(queue)-1]
			component = append(component, idx)
			if oldMask[idx] > 0.5 {
				overlap++
			}
			x := idx % w
			y := idx / w
			for dy := -1; dy <= 1; dy++ {
				ny := y + dy
				if ny < 0 || ny >= h {
					continue
				}
				for dx := -1; dx <= 1; dx++ {
					nx := x + dx
					if nx < 0 || nx >= w {
						continue
					}
					nidx := ny*w + nx
					if visited[nidx] != 0 || newMask[nidx] <= 0.5 {
						continue
					}
					visited[nidx] = 1
					queue = append(queue, nidx)
				}
			}
		}
		keep := true
		if len(component) > 0 && float64(overlap)/float64(len(component)) >= overlapRatio {
			keep = false
		}
		if keep {
			for _, idx := range component {
				out[idx] = 1
			}
		}
	}
	return out
}

// bestPerTile выбирает лучший снимок (min cloud) для каждого tile_id.
func bestPerTile(tiles []SafeTile) map[string]SafeTile {
	best := make(map[string]SafeTile)
	for _, t := range tiles {
		id := strings.TrimSpace(t.TileID)
		if id == "" {
			id = tileIDFromImgDataPath(t.ImgDataPath)
		}
		cur, ok := best[id]
		if !ok || t.Cloud < cur.Cloud {
			best[id] = t
		}
	}
	return best
}
