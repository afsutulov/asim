package main

// FinalizeSpec заполняет значения по умолчанию в спецификации модели.
// Нужен, чтобы модельные спецификации могли быть лаконичными, а пайплайн
// всегда получал валидные параметры.
//
// Логика:
//   - если ONNXFile не задан, берём <Name>.onnx
//   - если Tile не задан, ставим 256
//   - Bound приводим к неотрицательному значению
//   - если Preprocess пустой, ставим "sentinel"
//   - Inputs по умолчанию 1
//   - Simplify по умолчанию 0 (без упрощения)
//   - Divisor НЕ берётся из JSON. Для проекта sentinel он всегда 10000
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
	if m.Inputs <= 0 {
		m.Inputs = 1
	}
	m.Divisor = 10000
	return m
}
