package main

import "sort"

// ModelSpec описывает, как прогонять конкретную модель.
type ModelSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	ONNXFile    string   `json:"onnx_file"`
	Channels    []string `json:"channels"`
	Poligons    []string `json:"poligons,omitempty"`
	Tile        int      `json:"tile"`
	Bound       int      `json:"bound"`
	Threshold   float32  `json:"threshold"`
	Divisor     float32  `json:"-"`
	Preprocess  string   `json:"preprocess"`
	Inputs      int      `json:"inputs,omitempty"`
	Simplify    float64  `json:"simplify,omitempty"`
	MinArea     float64  `json:"min_area,omitempty"`
}

var activeModelSpecs = map[string]ModelSpec{}

func SetModelSpecs(specs map[string]ModelSpec) { activeModelSpecs = specs }

func GetModelSpec(name string) (ModelSpec, bool) {
	m, ok := activeModelSpecs[name]
	if !ok {
		return ModelSpec{}, false
	}
	return m, true
}

func ListModelNames() []string {
	out := make([]string, 0, len(activeModelSpecs))
	for k := range activeModelSpecs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
