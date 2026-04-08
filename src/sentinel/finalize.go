package main

// FinalizeSpec заполняет значения по умолчанию в спецификации модели.
//
// Логика:
//   - если ONNXFile не задан, берём <n>.onnx
//   - если Tile не задан, ставим 256
//   - Bound приводим к неотрицательному значению
//   - если Preprocess пустой, ставим "sentinel"
//   - Inputs по умолчанию 1
//   - NumClasses по умолчанию 1 (бинарная модель)
//   - Divisor по умолчанию 10000
//   - Merge берётся напрямую из config.json: true включает dissolve, false (и отсутствие поля) — нет
func FinalizeSpec(m ModelSpec) ModelSpec {
    if m.ONNXFile == "" && m.Name != "" {
	m.ONNXFile = m.Name + ".onnx"
    }
    if m.Tile <= 0 {
	m.Tile = 256
    }
    if m.Bound < 0 {
	m.Bound = 0
    }
    if m.Threshold == 0 {
	m.Threshold = 0.5
    }
    if m.Preprocess == "" {
	m.Preprocess = "sentinel"
    }
    if m.Resolution == "" {
	m.Resolution = "R10m"
    }
    if m.Inputs <= 0 {
	m.Inputs = 1
    }
    if m.NumClasses <= 0 {
	m.NumClasses = 1
    }
    if m.MaskFilter.Connectivity != 8 {
	m.MaskFilter.Connectivity = 4
    }
    m.Divisor = 10000
    return m
}