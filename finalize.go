package main

// FinalizeSpec заполняет значения по умолчанию в спецификации модели.
// Нужен, чтобы модельные спецификации могли быть лаконичными, а пайплайн
// всегда получал валидные параметры.
//
// Логика:
// - если ONNXFile не задан, берём <Name>.onnx
// - если Tile не задан, ставим 256
// - если Divisor не задан или <=0, ставим 1
// - если Mode пустой, ставим "binary"
// - если OutChannels не задан, ставим 1
// - если Preprocess пустой, ставим "sentinel"
// - Bound приводим к неотрицательному значению
func FinalizeSpec(m ModelSpec) ModelSpec {
	if m.ONNXFile == "" {
		if m.Name != "" {
			m.ONNXFile = m.Name + ".onnx"
		}
	}
	if m.Tile <= 0 {
		m.Tile = 256
	}
	if m.Divisor <= 0 {
		m.Divisor = 1
	}
	if m.Mode == "" {
		m.Mode = "binary"
	}
	if m.OutChannels <= 0 {
		m.OutChannels = 1
	}
	if m.Preprocess == "" {
		m.Preprocess = "sentinel"
	}
	if m.Bound < 0 {
		m.Bound = 0
	}
	if m.Inputs <= 0 {
		m.Inputs = 1
	}
	return m
}
