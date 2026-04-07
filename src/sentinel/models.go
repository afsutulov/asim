package main

import (
	"fmt"
	"sort"
)

type MaskFilterSpec struct {
	Enabled      bool `json:"enabled"`
	MinArea      int  `json:"min_area"`
	Connectivity int  `json:"connectivity"`
}

// ModelSpec описывает, как прогонять конкретную модель.
type ModelSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	ONNXFile    string   `json:"onnx_file"`
	Channels    []string `json:"channels"`
	PoligonsOn  []string `json:"poligons-on,omitempty"`  // области где искать (пересечение с --poligon)
	PoligonsOff []string `json:"poligons-off,omitempty"` // области куда результат НЕ должен попадать
	Tile        int      `json:"tile"`
	Bound       int      `json:"bound"`
	Threshold   float32  `json:"threshold"`
	Divisor     float32  `json:"-"`
	Preprocess  string   `json:"preprocess"`
	Resolution  string   `json:"resolution,omitempty"`
	Inputs      int      `json:"inputs,omitempty"`
	Simplify    float64  `json:"simplify,omitempty"`
	MinArea     float64  `json:"min_area,omitempty"`

	PreserveNativeResolution bool           `json:"preserve_native_resolution,omitempty"`
	Merge                    bool           `json:"merge,omitempty"`
	MaskFilter               MaskFilterSpec `json:"mask_filter,omitempty"`

	// Для многоклассовых моделей (например forest_disease_v3):
	// NumClasses > 1 → выход модели [N, NumClasses, H, W], argmax вместо threshold.
	// ChannelMeans — среднее по каналу, используется для заполнения nodata-пикселей.
	// Overlap — перекрытие тайлов в пикселях (альтернатива bound).
	// DetectionClasses — список значений argmax которые считаются детекцией.
	// Если пусто — детекцией считается любой класс > 0 (поведение по умолчанию).
	NumClasses       int       `json:"num_classes,omitempty"`
	ChannelMeans     []float64 `json:"channel_means,omitempty"`
	Overlap          int       `json:"overlap,omitempty"`
	DetectionClasses []int     `json:"detection_classes,omitempty"`

	// PairMode задаёт логику использования двух временных снимков при inputs > 1.
	// Допустимые значения:
	//   "concat"     — каналы обоих снимков объединяются и подаются в модель как
	//                  один тензор [N, C*2, H, W]. Поведение по умолчанию.
	//   "newer_only" — используется только новый (primary) снимок; второй (base)
	//                  игнорируется. Подходит для моделей, которые принимают один
	//                  снимок, но требуют двух периодов лишь для выбора сцены.
	//   "union"      — каждый снимок прогоняется через модель независимо,
	//                  результаты объединяются через union полигонов.
	// Указывать поле при inputs == 1 запрещено — конфиг не загрузится.
	PairMode string `json:"pair_mode,omitempty"`

	// BestScenePerTile: если true, из нескольких снимков одного tile_id за период
	// берётся только один — с минимальной облачностью (при равной — самый поздний).
	BestScenePerTile bool `json:"best_scene_per_tile,omitempty"`

	// CloudMaskMode задаёт набор факторов облачной маски при чтении тайла.
	// Допустимые значения:
	//   ""  или "full"   — полный фильтр: B11,B12,B10 + F1,F2,F3 (дефолт).
	//   "simple"         — только B11,B12,B10 (как в kosmo pathology_v2).
	//   "none"           — маска не применяется вообще.
	CloudMaskMode string `json:"cloud_mask_mode,omitempty"`
}

// IsMulticlass возвращает true если модель многоклассовая (argmax, не threshold).
func (s ModelSpec) IsMulticlass() bool {
	return s.NumClasses > 1
}

// effectivePairMode возвращает нормализованный режим работы с парой снимков.
func (s ModelSpec) effectivePairMode() string {
	switch s.PairMode {
	case "", "concat":
		return "concat"
	case "newer_only", "union":
		return s.PairMode
	default:
		return s.PairMode
	}
}

// ValidateSpec проверяет корректность ModelSpec.
func ValidateSpec(m ModelSpec) error {
	if m.Inputs <= 1 {
		if m.PairMode != "" {
			return fmt.Errorf(
				"model %q: pair_mode=%q указан, но inputs=%d — поле используется только при inputs > 1",
				m.Name, m.PairMode, m.Inputs,
			)
		}
		return nil
	}
	switch m.PairMode {
	case "", "concat", "newer_only", "union":
	default:
		return fmt.Errorf(
			"model %q: unknown pair_mode %q (allowed: concat, newer_only, union)",
			m.Name, m.PairMode,
		)
	}
	return nil
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
