package main

// FinalizeSpec заполняет значения по умолчанию в спецификации модели.
// Нужен, чтобы модельные спецификации могли быть лаконичными, а пайплайн
// всегда получал валидные параметры.
//
// Логика:
// - если ONNXFile не задан, берём <Name>.onnx
// - если Tile не задан, ставим 256
// - Bound приводим к неотрицательному значению
// - если Preprocess пустой, ставим "sentinel"
// - Inputs по умолчанию 1
// - Simplify по умолчанию 0 (без упрощения)
// - Divisor НЕ берётся из JSON, а вычисляется по Preprocess:
//     "sentinel" -> 10000, всё остальное -> 1
func FinalizeSpec(m ModelSpec) ModelSpec {
    // ONNX-файл по умолчанию — <name>.onnx
    if m.ONNXFile == "" && m.Name != "" {
	m.ONNXFile = m.Name + ".onnx"
    }

    // Тайл и bound
    if m.Tile <= 0 {
	m.Tile = 256
    }
    if m.Bound < 0 {
	m.Bound = 0
    }

    // Порог, если вдруг не задан
    if m.Threshold == 0 {
	m.Threshold = 0.5
    }

    // Preprocess по умолчанию
    if m.Preprocess == "" {
	m.Preprocess = "sentinel"
    }

    // Количество входов
    if m.Inputs <= 0 {
	m.Inputs = 1
    }

    // Simplify: 0 — это и есть значение "по умолчанию", ничего делать не нужно

    // ВАЖНО: divisor зависит только от preprocess и не читается из JSON
    if m.Preprocess == "sentinel" {
	m.Divisor = 10000
    } else {
	m.Divisor = 1
    }

    return m
}
