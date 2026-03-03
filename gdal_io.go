package main

import (
	"fmt"

	"github.com/lukeroth/gdal"
)

// ReadGeoTIFF9 читает GeoTIFF с 9 каналами Sentinel-2 (B04,B03,B02,B01,B05,B06,B08,B11,B12)
// и возвращает массив в формате img[band][0][y][x] (как в python пайплайне).
// ReadGeoTIFF9 читает GeoTIFF с 13 каналами Sentinel-2
// (B04,B03,B02,B01,B05,B06,B07,B08,B09,B10,B11,B12,B8A)
// и возвращает массив в формате img[band][0][y][x] (как в python пайплайне).
func ReadGeoTIFF9(path string) ([][][][]float32, [6]float64, string, error) {
	ds, err := gdal.Open(path, gdal.ReadOnly)
	if err != nil {
		return nil, [6]float64{}, "", err
	}
	defer ds.Close()

	w := ds.RasterXSize()
	h := ds.RasterYSize()
	if w <= 0 || h <= 0 {
		return nil, [6]float64{}, "", fmt.Errorf("invalid raster size %dx%d", w, h)
	}

	gt := ds.GeoTransform()
	var geo [6]float64
	copy(geo[:], gt[:])

	proj := ds.Projection()

	const numBands = 13
	if ds.RasterCount() < numBands {
		return nil, [6]float64{}, "", fmt.Errorf("expected >=%d bands, got %d", numBands, ds.RasterCount())
	}

	out := make([][][][]float32, numBands)
	for c := 0; c < numBands; c++ {
		band := ds.RasterBand(c + 1)
		buf := make([]float32, w*h)
		if err = band.IO(gdal.Read, 0, 0, w, h, buf, w, h, 0, 0); err != nil {
			return nil, [6]float64{}, "", err
		}
		im := make([][][]float32, 1)
		im[0] = make([][]float32, h)
		for y := 0; y < h; y++ {
			row := make([]float32, w)
			copy(row, buf[y*w:(y+1)*w])
			im[0][y] = row
		}
		out[c] = make([][][]float32, 1)
		out[c][0] = im[0]
	}
	return out, geo, proj, nil
}

// SelectChannels формирует тензор из нужных каналов (Bxx) из входного Sentinel-2 изображения.
// Возвращает imgC[c][0][y][x], где c соответствует order в spec.Channels.
func SelectChannels(img [][][][]float32, channels []string) ([][][][]float32, error) {
	out := make([][][][]float32, len(channels))
	for i, ch := range channels {
		// Производный канал NDVOG для forest_disease: считаем из B08 и B04.
		if ch == "NDVOG" {
			red := img[int(B04)][0]
			nir := img[int(B08)][0]
			h := len(red)
			w := len(red[0])
			one := make([][][]float32, 1)
			one[0] = make([][]float32, h)
			for y := 0; y < h; y++ {
				row := make([]float32, w)
				for x := 0; x < w; x++ {
					r := red[y][x]
					n := nir[y][x]
					den := n + r
					nd := float32(0)
					if den != 0 {
						nd = (n - r) / den // NDVI-like [-1..1]
					}
					// Переводим в [0..1], чтобы mean=0.5/std=0.5 соответствовали python preprocess.
					row[x] = (nd + 1) * 0.5
				}
				one[0][y] = row
			}
			out[i] = one
			continue
		}

		idx, ok := BandNameToIndex(ch)
		if !ok || idx < 0 || idx >= len(img) {
			return nil, fmt.Errorf("unsupported channel %s for current input", ch)
		}
		out[i] = img[idx]
	}
	return out, nil
}

// WriteGeoTIFF1 пишет одноканальную float32 маску (0/1) в GeoTIFF, сохраняя geotransform и проекцию.
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
