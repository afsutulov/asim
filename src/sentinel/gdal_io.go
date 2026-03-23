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
	needed := append([]string{}, channels...)
	needed = append(needed, "B02", "B03", "B04", "B10", "B11", "B12")
	seen := map[string]struct{}{}
	var best *bandRaster
	for _, ch := range needed {
		if strings.TrimSpace(ch) == "" {
			continue
		}
		if ch == "NDVOG" {
			ch = "B08"
		}
		ch = normalizeBandName(ch)
		if _, ok := seen[ch]; ok {
			continue
		}
		seen[ch] = struct{}{}
		br, err := readSingleBand(bandFile(tile, ch))
		if err != nil {
			return nil, err
		}
		if best == nil || br.w*br.h > best.w*best.h {
			best = br
		}
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

func needsRadioOffset(dateOnly string) bool {
	ts, err := time.Parse("2006-01-02", dateOnly)
	if err != nil {
		return false
	}
	return ts.After(time.Date(2022, 1, 25, 0, 0, 0, 0, time.UTC))
}

func buildCloudMask(bands map[string][]float32, n int) []uint8 {
	mask := make([]uint8, n)
	required := []string{"B02", "B03", "B04", "B10", "B11", "B12"}
	for _, key := range required {
		if _, ok := bands[key]; !ok {
			return mask
		}
	}
	for i := 0; i < n; i++ {
		b02 := bands["B02"][i]
		b03 := bands["B03"][i]
		b04 := bands["B04"][i]
		b10 := bands["B10"][i]
		b11 := bands["B11"][i]
		b12 := bands["B12"][i]
		f1 := float32(0)
		if den := b03 + b11; den != 0 {
			f1 = (b03 - b11) / den
		}
		f2 := (b02 + b03 + b04) / 3.0
		f3 := b02 - 0.5*b04 - 0.08
		if b11 < sentinelCloudThresholds["B11"] ||
			b12 < sentinelCloudThresholds["B12"] ||
			b10 > sentinelCloudThresholds["B10"] ||
			f1 > sentinelCloudThresholds["F1"] ||
			f2 > sentinelCloudThresholds["F2"] ||
			f3 > sentinelCloudThresholds["F3"] {
			mask[i] = 1
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
	}
	return true
}

func ReadSentinelTileCube(tile SafeTile, channels []string) (*RasterCube, error) {
	ref, err := chooseReferenceBand(tile, channels)
	if err != nil {
		return nil, err
	}
	out := make([][][][]float32, 0, len(channels))
	cache := map[string]*bandRaster{}
	resampled := map[string][]float32{}
	get := func(ch string) (*bandRaster, error) {
		ch = normalizeBandName(ch)
		if br, ok := cache[ch]; ok {
			return br, nil
		}
		br, err := readSingleBand(bandFile(tile, ch))
		if err != nil {
			return nil, err
		}
		cache[ch] = br
		return br, nil
	}
	getResampledTOA := func(ch string) ([]float32, error) {
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
			arr[i] = sentinelTOAValue(arr[i], tile.Date)
		}
		resampled[ch] = arr
		return arr, nil
	}

	validMask := make([]uint8, ref.w*ref.h)
	for i := range validMask {
		validMask[i] = 1
	}
	if useCloudMaskForChannels(channels) {
		maskBands := []string{"B02", "B03", "B04", "B10", "B11", "B12"}
		maskInput := make(map[string][]float32, len(maskBands))
		for _, ch := range maskBands {
			arr, err := getResampledTOA(ch)
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
		if ch == "NDVOG" {
			red, err := getResampledTOA("B04")
			if err != nil {
				return nil, err
			}
			nir, err := getResampledTOA("B08")
			if err != nil {
				return nil, err
			}
			arr = make([]float32, ref.w*ref.h)
			for i := range arr {
				if validMask[i] == 0 || red[i] == 0 || nir[i] == 0 {
					arr[i] = 0
					continue
				}
				den := nir[i] + red[i]
				if den != 0 {
					arr[i] = ((nir[i]-red[i])/den + 1) * 0.5
				}
			}
		} else {
			var err error
			arr, err = getResampledTOA(ch)
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
	band.SetNoDataValue(0)
	return band.IO(gdal.Write, 0, 0, w, h, data, w, h, 0, 0)
}
