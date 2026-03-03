package main

import (
    "errors"
    "fmt"
    "os"
    "path/filepath"
)

// clip01 ограничивает значение в диапазоне [0,1].
func clip01(x float32) float32 {
    if x < 0 {
	return 0
    }
    if x > 1 {
	return 1
    }
    return x
}

// preprocess выполняет нормализацию "как в kosmomonitoring": v/divisor + clamp.
// В исходном python для большинства Sentinel-моделей используется деление на 10000.
func preprocess(v float32, divisor float32) float32 {
    return clip01(v / divisor)
}

// preprocessBySpec применяет предобработку в зависимости от модели.
// - "ndvog": индекс уже в [0..1], нормализуем как в forest_disease.py: (x-0.5)/0.5
// - "none"/"raw": без нормализации (используем исходные значения пикселей)
// - всё остальное (включая "sentinel", "rgb"): делим на Divisor и режем в [0,1]
func preprocessBySpec(v float32, spec ModelSpec) float32 {
    switch spec.Preprocess {
    case "ndvog":
	// для индексов типа NDVOG, которые уже в [0..1]
	return (v - 0.5) / 0.5
    case "none", "raw":
	// без нормализации: как в forest_disease_v3.process,
	// где raster_data подаётся в сеть напрямую
	return v
    default:
	// sentinel/rgb (по умолчанию): делим на divisor и режем в [0..1]
	return preprocess(v, spec.Divisor)
    }
}

// RunModel выполняет полный пайплайн:
// 1) читает 9-канальный Sentinel GeoTIFF
// 2) выбирает нужные каналы под модель
// 3) режет изображение на тайлы, прогоняет ONNX модель батчами
// 4) собирает итоговую бинарную маску (0/1)
// 5) сохраняет либо GeoTIFF, либо SHP (через polygonize)
func RunModel(inTif, onnxModel, outPath string, batchSize int, device string, cudaDeviceID int, format string, minArea float64, simplify float64, spec ModelSpec) error {
    if batchSize < 1 {
	batchSize = 1
    }
    if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
	return err
    }

    img9, geo, proj, err := ReadGeoTIFF9(inTif)
    if err != nil {
	return err
    }

    img, err := SelectChannels(img9, spec.Channels)
    if err != nil {
	return err
    }

    // img[c][0][y][x]
    if len(img) == 0 || len(img[0]) == 0 || len(img[0][0]) == 0 || len(img[0][0][0]) == 0 {
	return errors.New("empty input image")
    }
    h := len(img[0][0])
    w := len(img[0][0][0])

    sess, err := NewORTSession(onnxModel, device, cudaDeviceID)
    if err != nil {
	return err
    }
    defer sess.Close()

    out := make([]float32, h*w)

    tileH := spec.Tile
    tileW := spec.Tile
    bound := spec.Bound
    stepY := tileH - 2*bound
    stepX := tileW - 2*bound
    if stepY <= 0 || stepX <= 0 {
	return fmt.Errorf("invalid bound=%d for tile=%dx%d", bound, tileH, tileW)
    }
    inChannels := len(spec.Channels)

    type tileMeta struct {
	wy0, wy1 int // рабочая область по Y в глобальных координатах
	wx0, wx1 int // рабочая область по X в глобальных координатах
	cy0, cx0 int // смещение рабочей области внутри тайла
    }

    var metas []tileMeta
    var batchInput []float32 // B*C*H*W
    var batchValid []uint8   // B*H*W

    flush := func() error {
	if len(metas) == 0 {
	    return nil
	}
	preds, err := sess.Predict(batchInput, len(metas), inChannels, tileH, tileW, spec.OutChannels)
	if err != nil {
	    return err
	}

	pixelCount := tileH * tileW

	for bi, m := range metas {
	    // базовый сдвиг по батчу
	    base := bi * spec.OutChannels * pixelCount

	    for yy := m.wy0; yy < m.wy1; yy++ {
		for xx := m.wx0; xx < m.wx1; xx++ {
		    py := (yy - m.wy0) + m.cy0
		    px := (xx - m.wx0) + m.cx0

		    // смещение пикселя внутри тайла
		    pOff := py*tileW + px

		    // индекс валидности
		    vIdx := bi*pixelCount + pOff
		    if batchValid[vIdx] == 0 {
			out[yy*w+xx] = 0
			continue
		    }

		    if spec.Mode == "argmax" && spec.OutChannels > 1 {
			// МНОГОКЛАССОВЫЙ ВЫХОД: [C,H,W] → argmax по каналам.
			maxVal := float32(0)
			maxClass := 0
			for c := 0; c < spec.OutChannels; c++ {
			    idx := base + c*pixelCount + pOff
			    v := preds[idx]
			    if c == 0 || v > maxVal {
				maxVal = v
				maxClass = c
			    }
			}
			// forest_disease_v3: 0 = фон/здоровый лес, 1..4 = патологии.
			if maxClass > 0 {
			    out[yy*w+xx] = 1
			} else {
			    out[yy*w+xx] = 0
			}
		    } else {
			// БИНАРНЫЙ ВЫХОД (OutChannels == 1) — старая логика.
			// В preds в этом случае хранятся значения единственного канала.
			idx := base + pOff // при OutChannels=1 base == bi*1*pixelCount
			if preds[idx] > spec.Threshold {
			    out[yy*w+xx] = 1
			} else {
			    out[yy*w+xx] = 0
			}
		    }
		}
	    }
	}

	metas = metas[:0]
	batchInput = batchInput[:0]
	batchValid = batchValid[:0]
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
		    sum := float32(0)
		    for c := 0; c < inChannels; c++ {
			val := preprocessBySpec(img[c][0][sy][sx], spec)
			patch[c*tileH*tileW+yy*tileW+xx] = val
			sum += val
		    }
		    if sum > 0 {
			valid[yy*tileW+xx] = 1
		    }
		}
	    }

	    // Глобальные границы тайла.
	    y1 := y0 + tileH
	    if y1 > h {
		y1 = h
	    }
	    x1 := x0 + tileW
	    if x1 > w {
		x1 = w
	    }

	    // Внутренняя (рабочая) область без bound.
	    innerY0 := y0 + bound
	    if innerY0 >= h {
		innerY0 = y0
	    }
	    innerY1 := y1 - bound
	    if innerY1 <= innerY0 {
		innerY1 = y1
	    }

	    innerX0 := x0 + bound
	    if innerX0 >= w {
		innerX0 = x0
	    }
	    innerX1 := x1 - bound
	    if innerX1 <= innerX0 {
		innerX1 = x1
	    }

	    // Если после расчёта рабочая область пустая – пропускаем этот тайл.
	    if innerY1 <= innerY0 || innerX1 <= innerX0 {
		continue
	    }

	    cy0 := innerY0 - y0
	    cx0 := innerX0 - x0

	    metas = append(metas, tileMeta{
		wy0: innerY0, wy1: innerY1,
		wx0: innerX0, wx1: innerX1,
		cy0: cy0, cx0: cx0,
	    })

	    batchInput = append(batchInput, patch...)
	    batchValid = append(batchValid, valid...)

	    if len(metas) >= batchSize {
		if err := flush(); err != nil {
		    return err
		}
	    }
	}
    }
    if err := flush(); err != nil {
	return err
    }

    // Всегда сначала пишем raster mask (он нужен и для tif, и как промежуточный для shp)
    maskTif := outPath
    if format == "shp" {
	maskTif = filepath.Join(filepath.Dir(outPath), spec.Name+"_mask.tif")
    }
    if err := WriteGeoTIFF1(maskTif, out, w, h, geo, proj); err != nil {
	return err
    }
    if format == "tif" {
	return nil
    }
    // format == "shp"
    if err := PolygonizeMaskToShapefile(maskTif, outPath, minArea, simplify); err != nil {
	return err
    }
    return nil
}
