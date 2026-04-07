package main

// dump.go — диагностический дамп входных и выходных данных модели.
//
// При запуске с флагом --dump-patch=/path/to/dir сохраняет:
//   patch_input.bin  — float32 NCHW тензор первого патча [1, C, H, W]
//   patch_output.bin — float32 тензор предсказания модели [1, 1, H, W]
//   patch_meta.txt   — метаданные: C, H, W, tile, date, channels
//
// Патч берётся из первого тайла первой пары (или первого тайла для inputs=1).
// Дамп происходит только один раз, потом отключается.
//
// Для проверки используйте скрипт check_patch.py (рядом с этим файлом).

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

// dumpDir задаётся через --dump-patch флаг в main.go.
var dumpDir string

// dumpDone — атомарный флаг, гарантирует что дамп происходит только один раз.
var dumpDone int32

// MaybeDumpPatch сохраняет патч и предсказание если дамп ещё не был сделан.
// patch: float32 NCHW для одного тайла [C*H*W], pred: float32 [H*W]
// channels, h, w: размерность, tileLabel: имя тайла для метаданных.
func MaybeDumpPatch(patch, pred []float32, channels, h, w int, tileLabel string, chanNames []string) {
	if dumpDir == "" {
		return
	}
	if !atomic.CompareAndSwapInt32(&dumpDone, 0, 1) {
		return // уже сохранили
	}
	if err := os.MkdirAll(dumpDir, 0o755); err != nil {
		log.Printf("dump: mkdir failed: %v", err)
		return
	}

	// Сохраняем входной тензор
	if err := writeFloat32Bin(filepath.Join(dumpDir, "patch_input.bin"), patch); err != nil {
		log.Printf("dump: write input failed: %v", err)
		return
	}

	// Сохраняем предсказание модели
	if err := writeFloat32Bin(filepath.Join(dumpDir, "patch_output.bin"), pred); err != nil {
		log.Printf("dump: write output failed: %v", err)
		return
	}

	// Считаем статистику входа
	inMin, inMax, inMean := statsFloat32(patch)
	outMin, outMax, outMean := statsFloat32(pred)

	// Метаданные
	meta := fmt.Sprintf(
		"tile: %s\nchannels: %d\nheight: %d\nwidth: %d\nchan_names: %s\n\n"+
			"input  min=%.6f max=%.6f mean=%.8f\noutput min=%.6f max=%.6f mean=%.8f\n",
		tileLabel, channels, h, w, strings.Join(chanNames, ","),
		inMin, inMax, inMean,
		outMin, outMax, outMean,
	)
	if err := os.WriteFile(filepath.Join(dumpDir, "patch_meta.txt"), []byte(meta), 0o644); err != nil {
		log.Printf("dump: write meta failed: %v", err)
		return
	}
	log.Printf("dump: saved patch to %s (C=%d H=%d W=%d input_max=%.4f output_max=%.6f)",
		dumpDir, channels, h, w, inMax, outMax)
}

func writeFloat32Bin(path string, data []float32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 4)
	for _, v := range data {
		binary.LittleEndian.PutUint32(buf, math.Float32bits(v))
		if _, err := f.Write(buf); err != nil {
			return err
		}
	}
	return nil
}

func statsFloat32(data []float32) (min, max, mean float32) {
	if len(data) == 0 {
		return
	}
	min, max = data[0], data[0]
	var sum float64
	for _, v := range data {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += float64(v)
	}
	mean = float32(sum / float64(len(data)))
	return
}
