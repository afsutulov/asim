package main

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lukeroth/gdal"
)

type RasterCube struct {
	Data      [][][][]float32
	Geo       [6]float64
	Proj      string
	W         int
	H         int
	ValidMask []uint8
}

type bandRaster struct {
	data []float32
	geo  [6]float64
	proj string
	w, h int
}

const sentinelNoData = float32(0)
const maxNoDataPercent = 99.9

var sentinelCloudThresholds = map[string]float32{
	"B11": 0.03,
	"B12": 0.02,
	"B10": 0.0025,
	"F1":  0.1,
	"F2":  0.25,
	"F3":  0.01,
}

func firstJP2(imgData string) (string, error) {
	entries, err := os.ReadDir(imgData)
	if err != nil {
		return "", err
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := strings.ToUpper(ent.Name())
		if strings.HasSuffix(strings.ToLower(ent.Name()), ".jp2") && !strings.Contains(name, "_TCI") {
			return filepath.Join(imgData, ent.Name()), nil
		}
	}
	return "", fmt.Errorf("jp2 not found in %s", imgData)
}

func tileGeoFromIMGData(imgData string) ([6]float64, int, int, string, [4]float64, error) {
	var geo [6]float64
	var env [4]float64
	p, err := firstJP2(imgData)
	if err != nil {
		return geo, 0, 0, "", env, err
	}
	ds, err := gdal.Open(p, gdal.ReadOnly)
	if err != nil {
		return geo, 0, 0, "", env, err
	}
	defer ds.Close()
	gt := ds.GeoTransform()
	copy(geo[:], gt[:])
	w, h := ds.RasterXSize(), ds.RasterYSize()
	proj := ds.Projection()
	xs := []float64{geo[0], geo[0] + float64(w)*geo[1], geo[0] + float64(h)*geo[2], geo[0] + float64(w)*geo[1] + float64(h)*geo[2]}
	ys := []float64{geo[3], geo[3] + float64(w)*geo[4], geo[3] + float64(h)*geo[5], geo[3] + float64(w)*geo[4] + float64(h)*geo[5]}
	minx, maxx := xs[0], xs[0]
	miny, maxy := ys[0], ys[0]
	for i := 1; i < 4; i++ {
		if xs[i] < minx {
			minx = xs[i]
		}
		if xs[i] > maxx {
			maxx = xs[i]
		}
		if ys[i] < miny {
			miny = ys[i]
		}
		if ys[i] > maxy {
			maxy = ys[i]
		}
	}
	env = [4]float64{minx, miny, maxx, maxy}
	return geo, w, h, proj, env, nil
}

func readSingleBand(path string) (*bandRaster, error) {
	ds, err := gdal.Open(path, gdal.ReadOnly)
	if err != nil {
		return nil, err
	}
	defer ds.Close()
	w, h := ds.RasterXSize(), ds.RasterYSize()
	if ds.RasterCount() < 1 || w <= 0 || h <= 0 {
		return nil, fmt.Errorf("invalid raster %s", path)
	}
	buf := make([]float32, w*h)
	if err := ds.RasterBand(1).IO(gdal.Read, 0, 0, w, h, buf, w, h, 0, 0); err != nil {
		return nil, err
	}
	var geo [6]float64
	gt := ds.GeoTransform()
	copy(geo[:], gt[:])
	return &bandRaster{data: buf, geo: geo, proj: ds.Projection(), w: w, h: h}, nil
}

func chooseReferenceBand(tile SafeTile, channels []string) (*bandRaster, error) {
	return chooseReferenceBandWithSpec(tile, channels, ModelSpec{})
}

func chooseReferenceBandWithSpec(tile SafeTile, channels []string, spec ModelSpec) (*bandRaster, error) {
	needed := append([]string{}, channels...)
	needed = append(needed, referenceBandCandidates(spec)...)
	seen := map[string]struct{}{}
	var best *bandRaster
	for _, ch := range needed {
		if strings.TrimSpace(ch) == "" {
			continue
		}
		norm := normalizeBandName(ch)
		if strings.HasPrefix(norm, "NDVI") || norm == "NDVOG" {
			norm = "B08"
		}
		if _, ok := seen[norm]; ok {
			continue
		}
		seen[norm] = struct{}{}
		br, err := readSingleBand(bandFileForSpec(tile, norm, spec))
		if err != nil {
			continue
		}
		if best == nil {
			best = br
			continue
		}
		if normalizePreprocess(spec.Preprocess) == "sentinel2a" {
			if br.w == best.w && br.h == best.h {
				best = br
			}
			continue
		}
		if br.w*br.h > best.w*best.h {
			best = br
		}
	}
	if best == nil {
		return nil, fmt.Errorf("reference band not found for %s", tileLabel(tile))
	}
	return best, nil
}

func bilinearResample(src *bandRaster, ref *bandRaster) []float32 {
	if src.w == ref.w && src.h == ref.h {
		out := make([]float32, len(src.data))
		copy(out, src.data)
		return out
	}
	out := make([]float32, ref.w*ref.h)
	xScale := float64(src.w) / float64(ref.w)
	yScale := float64(src.h) / float64(ref.h)
	for y := 0; y < ref.h; y++ {
		sy := (float64(y)+0.5)*yScale - 0.5
		y0 := int(math.Floor(sy))
		y1 := y0 + 1
		fy := float32(sy - float64(y0))
		if y0 < 0 {
			y0 = 0
		}
		if y1 >= src.h {
			y1 = src.h - 1
		}
		for x := 0; x < ref.w; x++ {
			sx := (float64(x)+0.5)*xScale - 0.5
			x0 := int(math.Floor(sx))
			x1 := x0 + 1
			fx := float32(sx - float64(x0))
			if x0 < 0 {
				x0 = 0
			}
			if x1 >= src.w {
				x1 = src.w - 1
			}
			v00 := src.data[y0*src.w+x0]
			v10 := src.data[y0*src.w+x1]
			v01 := src.data[y1*src.w+x0]
			v11 := src.data[y1*src.w+x1]
			if v00 == sentinelNoData || v10 == sentinelNoData || v01 == sentinelNoData || v11 == sentinelNoData {
				out[y*ref.w+x] = sentinelNoData
				continue
			}
			v0 := v00*(1-fx) + v10*fx
			v1 := v01*(1-fx) + v11*fx
			out[y*ref.w+x] = v0*(1-fy) + v1*fy
		}
	}
	return out
}

func normalizeBandName(ch string) string { return strings.ToUpper(strings.TrimSpace(ch)) }

func sentinelTOAValue(raw float32, dateOnly string) float32 {
	if raw == sentinelNoData {
		return sentinelNoData
	}
	v := raw
	if needsRadioOffset(dateOnly) {
		v = raw - 1000
	}
	if v < 0 {
		v = 0
	}
	v = v / 10000.0
	if v > 1 {
		v = 1
	}
	return v
}

func sentinelBOAValue(raw float32) float32 {
	if raw == sentinelNoData {
		return sentinelNoData
	}
	v := raw / 10000.0
	if v < 0 {
		v = 0
	}
	if v > 1 {
		v = 1
	}
	return v
}

func needsRadioOffset(dateOnly string) bool {
	ts, err := time.Parse("2006-01-02", dateOnly)
	if err != nil {
		return false
	}
	return ts.After(time.Date(2022, 1, 25, 0, 0, 0, 0, time.UTC))
}

func buildCloudMask(bands map[string][]float32, n int) []uint8 {
	mask := make([]uint8, n)
	// simple режим: только B11, B12, B10 (как в kosmo pathology_v2)
	if b10, ok10 := bands["B10"]; ok10 {
		b11, ok11 := bands["B11"]
		b12, ok12 := bands["B12"]
		if !ok11 || !ok12 {
			return mask
		}
		// Полный режим: дополнительно F1/F2/F3 на основе B02/B03/B04
		b02, hasB02 := bands["B02"]
		b03, hasB03 := bands["B03"]
		b04, hasB04 := bands["B04"]
		useFullFilter := hasB02 && hasB03 && hasB04
		for i := 0; i < n; i++ {
			cloud := b11[i] < sentinelCloudThresholds["B11"] ||
				b12[i] < sentinelCloudThresholds["B12"] ||
				b10[i] > sentinelCloudThresholds["B10"]
			if !cloud && useFullFilter {
				f1 := float32(0)
				if den := b03[i] + b11[i]; den != 0 {
					f1 = (b03[i] - b11[i]) / den
				}
				f2 := (b02[i] + b03[i] + b04[i]) / 3.0
				f3 := b02[i] - 0.5*b04[i] - 0.08
				cloud = f1 > sentinelCloudThresholds["F1"] ||
					f2 > sentinelCloudThresholds["F2"] ||
					f3 > sentinelCloudThresholds["F3"]
			}
			if cloud {
				mask[i] = 1
			}
		}
	}
	return mask
}

func useCloudMaskForChannels(channels []string) bool {
	if len(channels) == 4 {
		norm := make([]string, 4)
		for i, ch := range channels {
			norm[i] = normalizeBandName(ch)
		}
		if norm[0] == "B04" && norm[1] == "B03" && norm[2] == "B02" && norm[3] == "B08" {
			return false
		}

		if len(norm) == 3 &&
			norm[0] == "B12" &&
			norm[1] == "B8A" &&
			norm[2] == "B03" {
			return false
		}

	}
	return true
}

func ReadSentinelTileCube(tile SafeTile, channels []string, cloudMaskMode string) (*RasterCube, error) {
	return ReadSentinelTileCubeWithSpec(tile, channels, cloudMaskMode, ModelSpec{})
}

func ReadSentinelTileCubeWithSpec(tile SafeTile, channels []string, cloudMaskMode string, spec ModelSpec) (*RasterCube, error) {
	mode := normalizePreprocess(spec.Preprocess)
	ref, err := chooseReferenceBandWithSpec(tile, channels, spec)
	if err != nil {
		return nil, err
	}
	const maxTilePixels = 5490 * 5490
	if !spec.PreserveNativeResolution && ref.w*ref.h > maxTilePixels {
		fallbackBand := "B05"
		if mode == "sentinel2a" && normalizeResolution(spec.Resolution) == "R60m" {
			fallbackBand = "B01"
		}
		if refLow, errLow := readSingleBand(bandFileForSpec(tile, fallbackBand, spec)); errLow == nil {
			ref = refLow
		}
	}
	out := make([][][][]float32, 0, len(channels))
	cache := map[string]*bandRaster{}
	resampled := map[string][]float32{}
	get := func(ch string) (*bandRaster, error) {
		ch = normalizeBandName(ch)
		if br, ok := cache[ch]; ok {
			return br, nil
		}
		br, err := readSingleBand(bandFileForSpec(tile, ch, spec))
		if err != nil {
			return nil, err
		}
		cache[ch] = br
		return br, nil
	}
	getResampled := func(ch string) ([]float32, error) {
		ch = normalizeBandName(ch)
		if arr, ok := resampled[ch]; ok {
			return arr, nil
		}
		br, err := get(ch)
		if err != nil {
			return nil, err
		}
		arr := bilinearResample(br, ref)
		for i := range arr {
			if mode == "sentinel2a" {
				arr[i] = sentinelBOAValue(arr[i])
			} else {
				arr[i] = sentinelTOAValue(arr[i], tile.Date)
			}
		}
		resampled[ch] = arr
		return arr, nil
	}

	validMask := make([]uint8, ref.w*ref.h)
	for i := range validMask {
		validMask[i] = 1
	}
	dataMaskChannels := make([][]float32, 0, len(channels))
	for _, ch := range channels {
		norm := normalizeBandName(ch)
		if strings.HasPrefix(norm, "NDVI") || norm == "NDVOG" {
			red, err := getResampled("B04")
			if err != nil {
				return nil, err
			}
			nir, err := getResampled("B08")
			if err != nil {
				return nil, err
			}
			tmp := make([]float32, ref.w*ref.h)
			for i := range tmp {
				if red[i] > 0 || nir[i] > 0 {
					tmp[i] = 1
				}
			}
			dataMaskChannels = append(dataMaskChannels, tmp)
			continue
		}
		arr, err := getResampled(norm)
		if err != nil {
			return nil, err
		}
		dataMaskChannels = append(dataMaskChannels, arr)
	}
	for i := range validMask {
		hasData := false
		for _, arr := range dataMaskChannels {
			if arr[i] > 0 {
				hasData = true
				break
			}
		}
		if !hasData {
			validMask[i] = 0
		}
	}

	if mode == "sentinel2a" {
		sclPath := bandFileForSpec(tile, "SCL", spec)
		if !strings.Contains(filepath.Base(sclPath), "MISSING_SCL") {
			sclBr, err := readSingleBand(sclPath)
			if err == nil {
				scl := bilinearResample(sclBr, ref)
				for i, v := range scl {
					cls := int(v + 0.5)
					switch cls {
					case 0, 1, 3, 8, 9, 10, 11:
						validMask[i] = 0
					}
				}
			}
		}
	} else {
		effectiveCloudMode := strings.ToLower(strings.TrimSpace(cloudMaskMode))
		if effectiveCloudMode != "none" && useCloudMaskForChannels(channels) {
			var maskBands []string
			if effectiveCloudMode == "simple" {
				maskBands = []string{"B10", "B11", "B12"}
			} else {
				maskBands = []string{"B02", "B03", "B04", "B10", "B11", "B12"}
			}
			maskInput := make(map[string][]float32, len(maskBands))
			for _, ch := range maskBands {
				arr, err := getResampled(ch)
				if err != nil {
					return nil, err
				}
				maskInput[ch] = arr
			}
			cloudMask := buildCloudMask(maskInput, ref.w*ref.h)
			for i := range validMask {
				if cloudMask[i] != 0 {
					validMask[i] = 0
				}
			}
		}
	}

	validCount := 0
	for i := range validMask {
		if validMask[i] != 0 {
			validCount++
		}
	}
	noDataPercent := 100.0 * float64(ref.w*ref.h-validCount) / float64(ref.w*ref.h)
	if noDataPercent >= maxNoDataPercent {
		return nil, fmt.Errorf("too many nodata/cloud pixels: %.2f%%", noDataPercent)
	}

	for _, ch := range channels {
		ch = normalizeBandName(ch)
		var arr []float32
		if ch == "NDVOG" || strings.HasPrefix(ch, "NDVI") {
			red, err := getResampled("B04")
			if err != nil {
				return nil, err
			}
			nir, err := getResampled("B08")
			if err != nil {
				return nil, err
			}
			arr = make([]float32, ref.w*ref.h)
			for i := range arr {
				if validMask[i] == 0 || red[i] == 0 || nir[i] == 0 {
					continue
				}
				den := nir[i] + red[i]
				if den != 0 {
					arr[i] = ((nir[i]-red[i])/den + 1) * 0.5
				}
			}
		} else {
			arr, err = getResampled(ch)
			if err != nil {
				return nil, err
			}
			arr = append([]float32(nil), arr...)
			for i := range arr {
				if validMask[i] == 0 {
					arr[i] = 0
				}
			}
		}
		im := make([][][]float32, 1)
		im[0] = make([][]float32, ref.h)
		for y := 0; y < ref.h; y++ {
			row := make([]float32, ref.w)
			copy(row, arr[y*ref.w:(y+1)*ref.w])
			im[0][y] = row
		}
		out = append(out, im)
	}
	return &RasterCube{Data: out, Geo: ref.geo, Proj: ref.proj, W: ref.w, H: ref.h, ValidMask: validMask}, nil
}

func selectBestTileForInterval(tiles []SafeTile, start, end time.Time) (SafeTile, bool) {
	var best SafeTile
	found := false
	for _, tile := range tiles {
		t := tileTime(tile)
		if t.Before(start) || t.After(end) {
			continue
		}
		if !found || tile.Cloud < best.Cloud || (tile.Cloud == best.Cloud && t.After(tileTime(best))) {
			best = tile
			found = true
		}
	}
	if found {
		return best, true
	}
	mid := start.Add(end.Sub(start) / 2)
	bestDist := time.Duration(1<<63 - 1)
	for _, tile := range tiles {
		d := tileTime(tile).Sub(mid)
		if d < 0 {
			d = -d
		}
		if !found || d < bestDist || (d == bestDist && tile.Cloud < best.Cloud) {
			best = tile
			bestDist = d
			found = true
		}
	}
	return best, found
}

func buildIntervals(start, end string, n int) ([][2]time.Time, error) {
	if n <= 0 {
		return nil, fmt.Errorf("invalid interval count: %d", n)
	}
	startDate, err := parseDateOnly(start)
	if err != nil {
		return nil, err
	}
	endDate, err := parseDateOnly(end)
	if err != nil {
		return nil, err
	}
	if endDate.Before(startDate) {
		return nil, fmt.Errorf("end date before start date")
	}
	endExclusive := endDate.Add(24 * time.Hour)
	dur := endExclusive.Sub(startDate)
	step := dur / time.Duration(n)
	if step <= 0 {
		step = 24 * time.Hour
	}
	out := make([][2]time.Time, n)
	cur := startDate
	for i := 0; i < n; i++ {
		next := cur.Add(step)
		if i == n-1 || next.After(endExclusive) {
			next = endExclusive
		}
		out[i] = [2]time.Time{cur, next.Add(-time.Nanosecond)}
		cur = next
	}
	return out, nil
}

func BuildNDVITimeSeriesCube(tiles []SafeTile, spec ModelSpec, start, end string) (*RasterCube, error) {
	if len(tiles) == 0 {
		return nil, fmt.Errorf("no tiles for NDVI time series")
	}
	intervals, err := buildIntervals(start, end, len(spec.Channels))
	if err != nil {
		return nil, err
	}
	ref, err := chooseReferenceBandWithSpec(tiles[0], []string{"B08"}, spec)
	if err != nil {
		return nil, err
	}
	const maxTilePixels = 5490 * 5490
	if !spec.PreserveNativeResolution && ref.w*ref.h > maxTilePixels {
		if refLow, errLow := readSingleBand(bandFileForSpec(tiles[0], "B05", spec)); errLow == nil {
			ref = refLow
		}
	}
	out := make([][][][]float32, 0, len(spec.Channels))
	validMask := make([]uint8, ref.w*ref.h)
	for i := range validMask {
		validMask[i] = 1
	}
	for _, iv := range intervals {
		tile, ok := selectBestTileForInterval(tiles, iv[0], iv[1])
		if !ok {
			arr := make([]float32, ref.w*ref.h)
			im := make([][][]float32, 1)
			im[0] = make([][]float32, ref.h)
			for y := 0; y < ref.h; y++ {
				row := make([]float32, ref.w)
				copy(row, arr[y*ref.w:(y+1)*ref.w])
				im[0][y] = row
			}
			out = append(out, im)
			continue
		}
		redBr, err := readSingleBand(bandFileForSpec(tile, "B04", spec))
		if err != nil {
			return nil, err
		}
		nirBr, err := readSingleBand(bandFileForSpec(tile, "B08", spec))
		if err != nil {
			return nil, err
		}
		red := bilinearResample(redBr, ref)
		nir := bilinearResample(nirBr, ref)
		arr := make([]float32, ref.w*ref.h)
		for i := range arr {
			redv := sentinelTOAValue(red[i], tile.Date)
			nirv := sentinelTOAValue(nir[i], tile.Date)
			if redv <= 0 && nirv <= 0 {
				validMask[i] = 0
				arr[i] = 0
				continue
			}
			den := nirv + redv
			if den != 0 {
				ndvi := (nirv - redv) / den
				if ndvi < 0 {
					ndvi = 0
				}
				arr[i] = ndvi
			}
		}
		im := make([][][]float32, 1)
		im[0] = make([][]float32, ref.h)
		for y := 0; y < ref.h; y++ {
			row := make([]float32, ref.w)
			copy(row, arr[y*ref.w:(y+1)*ref.w])
			im[0][y] = row
		}
		out = append(out, im)
	}
	return &RasterCube{Data: out, Geo: ref.geo, Proj: ref.proj, W: ref.w, H: ref.h, ValidMask: validMask}, nil
}

func cubeBounds(c *RasterCube) (float64, float64, float64, float64) {
	minx := c.Geo[0]
	maxy := c.Geo[3]
	maxx := c.Geo[0] + float64(c.W)*c.Geo[1]
	miny := c.Geo[3] + float64(c.H)*c.Geo[5]
	if maxx < minx {
		minx, maxx = maxx, minx
	}
	if maxy < miny {
		miny, maxy = maxy, miny
	}
	return minx, miny, maxx, maxy
}

func cropCube(c *RasterCube, x0, y0, w, h int) *RasterCube {
	if x0 < 0 {
		x0 = 0
	}
	if y0 < 0 {
		y0 = 0
	}
	if x0+w > c.W {
		w = c.W - x0
	}
	if y0+h > c.H {
		h = c.H - y0
	}
	if w <= 0 || h <= 0 {
		return nil
	}
	out := &RasterCube{
		Data:      make([][][][]float32, len(c.Data)),
		Geo:       c.Geo,
		Proj:      c.Proj,
		W:         w,
		H:         h,
		ValidMask: make([]uint8, w*h),
	}
	out.Geo[0] = c.Geo[0] + float64(x0)*c.Geo[1] + float64(y0)*c.Geo[2]
	out.Geo[3] = c.Geo[3] + float64(x0)*c.Geo[4] + float64(y0)*c.Geo[5]
	for i := range out.ValidMask {
		out.ValidMask[i] = 0
	}
	for yy := 0; yy < h; yy++ {
		srcOff := (y0+yy)*c.W + x0
		dstOff := yy * w
		copy(out.ValidMask[dstOff:dstOff+w], c.ValidMask[srcOff:srcOff+w])
	}
	for ch := range c.Data {
		im := make([][][]float32, 1)
		im[0] = make([][]float32, h)
		for yy := 0; yy < h; yy++ {
			row := make([]float32, w)
			copy(row, c.Data[ch][0][y0+yy][x0:x0+w])
			im[0][yy] = row
		}
		out.Data[ch] = im
	}
	return out
}

func alignedIntersectionCubes(a, b *RasterCube) (*RasterCube, *RasterCube, error) {
	if a == nil || b == nil {
		return nil, nil, fmt.Errorf("nil cube")
	}
	aminx, aminy, amaxx, amaxy := cubeBounds(a)
	bminx, bminy, bmaxx, bmaxy := cubeBounds(b)
	ixmin := math.Max(aminx, bminx)
	ixmax := math.Min(amaxx, bmaxx)
	iymin := math.Max(aminy, bminy)
	iymax := math.Min(amaxy, bmaxy)
	if !(ixmax > ixmin && iymax > iymin) {
		return nil, nil, fmt.Errorf("paired tiles do not intersect")
	}
	axRes := math.Abs(a.Geo[1])
	ayRes := math.Abs(a.Geo[5])
	bxRes := math.Abs(b.Geo[1])
	byRes := math.Abs(b.Geo[5])
	if math.Abs(axRes-bxRes) > 1e-6 || math.Abs(ayRes-byRes) > 1e-6 {
		return nil, nil, fmt.Errorf("paired tiles have different pixel size")
	}
	ax0 := int(math.Floor((ixmin-a.Geo[0])/axRes + 1e-9))
	ax1 := int(math.Ceil((ixmax-a.Geo[0])/axRes - 1e-9))
	ay0 := int(math.Floor((a.Geo[3]-iymax)/ayRes + 1e-9))
	ay1 := int(math.Ceil((a.Geo[3]-iymin)/ayRes - 1e-9))
	bx0 := int(math.Floor((ixmin-b.Geo[0])/bxRes + 1e-9))
	bx1 := int(math.Ceil((ixmax-b.Geo[0])/bxRes - 1e-9))
	by0 := int(math.Floor((b.Geo[3]-iymax)/byRes + 1e-9))
	by1 := int(math.Ceil((b.Geo[3]-iymin)/byRes - 1e-9))
	aw, ah := ax1-ax0, ay1-ay0
	bw, bh := bx1-bx0, by1-by0
	minW, minH := aw, ah
	if bw < minW {
		minW = bw
	}
	if bh < minH {
		minH = bh
	}
	if minW <= 0 || minH <= 0 {
		return nil, nil, fmt.Errorf("paired tiles have empty common raster area")
	}
	ca := cropCube(a, ax0, ay0, minW, minH)
	cb := cropCube(b, bx0, by0, minW, minH)
	if ca == nil || cb == nil {
		return nil, nil, fmt.Errorf("failed to crop paired tiles to common intersection")
	}
	if len(ca.ValidMask) == len(cb.ValidMask) {
		for i := range ca.ValidMask {
			if ca.ValidMask[i] == 0 || cb.ValidMask[i] == 0 {
				ca.ValidMask[i] = 0
				cb.ValidMask[i] = 0
				for ch := range ca.Data {
					ca.Data[ch][0][i/minW][i%minW] = 0
				}
				for ch := range cb.Data {
					cb.Data[ch][0][i/minW][i%minW] = 0
				}
			}
		}
	}
	return ca, cb, nil
}

// WriteGeoTIFF1 пишет одноканальную float32 маску (0/1) в GeoTIFF.
func WriteGeoTIFF1(path string, data []float32, w, h int, geo [6]float64, proj string) error {
	drv, err := gdal.GetDriverByName("GTiff")
	if err != nil {
		return err
	}
	ds := drv.Create(path, w, h, 1, gdal.Float32, nil)
	defer ds.Close()
	ds.SetGeoTransform(geo)
	if proj != "" {
		ds.SetProjection(proj)
	}
	band := ds.RasterBand(1)
	// Не устанавливаем NoDataValue: GDALPolygonize должен обрабатывать все пиксели
	// включая нули внутри областей детекции — это создаёт дыры в полигонах,
	// как в kosmo rasterio.features.shapes (полигонизация по всем значениям).
	// SetNoDataValue(0) заставляло GDAL игнорировать нулевые пиксели → нет дыр →
	// полигоны без дыр имеют большую площадь чем в kosmo.
	return band.IO(gdal.Write, 0, 0, w, h, data, w, h, 0, 0)
}
