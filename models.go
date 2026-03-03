package main

import (
	"encoding/json"
	"os"
	"sort"
)

// ModelSpec описывает, как прогонять конкретную модель:
// какие входные каналы использовать, какой порог бинаризации, какой bound (отступ) при склейке тайлов и т.д.
type ModelSpec struct {
    Name        string   `json:"name"`         // логическое имя модели (hogweed, forest, ...)
    Description string   `json:"description"`  // человекочитаемое описание модели
    ONNXFile    string   `json:"onnx_file"`    // имя файла модели в папке models/
    Channels    []string `json:"channels"`     // список каналов ("B04","B03","B02","B08" и т.п.)
    Tile        int      `json:"tile"`         // размер тайла (в пикселях)
    Bound       int      `json:"bound"`        // отступ, который не записываем по краям тайла
    Threshold   float32  `json:"threshold"`    // порог бинаризации
    Divisor     float32  `json:"divisor"`      // делитель для нормализации
    OutChannels int      `json:"out_channels"` // число каналов на выходе модели
    Mode        string   `json:"mode"`         // "binary" или "argmax"
    Preprocess  string   `json:"preprocess"`   // "sentinel", "rgb", "ndvog"

    // Новое:
    Inputs int `json:"inputs,omitempty"` // число ожидаемых GeoTIFF-входов: 1 (по умолчанию) или 2 для моделей "до/после" (windfall)
}

// activeModelSpecs — текущий набор моделей, загруженный из внешнего файла.
var activeModelSpecs = map[string]ModelSpec{}

// LoadModelSpecsFromFile загружает описания моделей из JSON-файла.
// Формат файла: объект JSON вида { "имя_модели": { ...ModelSpec... }, ... }.
// Поле Name внутри объекта можно не указывать — оно будет заполнено из ключа.
func LoadModelSpecsFromFile(path string) (map[string]ModelSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var raw map[string]ModelSpec
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	out := make(map[string]ModelSpec, len(raw))
	for key, spec := range raw {
		if spec.Name == "" {
			spec.Name = key
		}
		// Заполняем значения по умолчанию так же, как для встроенных моделей.
		out[key] = FinalizeSpec(spec)
	}

	return out, nil
}

// SetModelSpecs устанавливает текущий набор моделей.
func SetModelSpecs(specs map[string]ModelSpec) {
	activeModelSpecs = specs
}

// GetModelSpec возвращает спецификацию модели по имени.
func GetModelSpec(name string) (ModelSpec, bool) {
	m, ok := activeModelSpecs[name]
	if !ok {
		return ModelSpec{}, false
	}
	return m, true
}

// ListModelNames возвращает список поддерживаемых имён моделей (для справки пользователю).
func ListModelNames() []string {
	if len(activeModelSpecs) == 0 {
		return nil
	}
	out := make([]string, 0, len(activeModelSpecs))
	for k := range activeModelSpecs {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
