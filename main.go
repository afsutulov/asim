package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func printJsonFormat() {
	fmt.Fprintln(os.Stderr, `
Пример содержимого models.json:
{
  "hogweed": {        // Название модели
    "description": "Зарастание Борщевиком Сосновского", // Краткое описание модели
    "onnx_file": "hogweed.onnx",                        // файл xxx.onnx с моделью
    "channels": ["B04", "B03", "B02", "B08"],           // Перечень используемых моделью каналов
    "tile": 256,      // Размер тайла
    "bound": 32,      // Размер отступа от границ тайла при поиске
    "threshold": 0.6, // Порог срабатывания: 0.1 - все подозрительное считаем объектом; 0.5 - стандарт; 0.9 - строгий отбор
    "preprocess": "sentinel",                           // Алгоритм работы с моделью
    "inputs": 1,      // Количество используемых GeoTIFF на входе
    "simplify": 0.0   // Упрощение геометрии полигона: 0 - без изменений; 5 - сглаживание; 10 - нормальная оптимизация; >30 - сильное упрощение
    "min_area": 0.0   // Размер полигонов: 0 - ничего не фильструем; 100 - от 100кв.м.; 1 000 - от 1000кв.м. 10 000 - от 10000км.в.
  }
}
`)
}


func main() {
	inPath := flag.String("input", "", "Задается путь и имя GeoTIFF файла на обработку")
	inPath2 := flag.String("input2", "", "Опционально. Задается путь и имя второго GeoTIFF. Если модель должна сранивать два снимка: ОТ и ДО (значение поля \"input\" в описании моделе равно 2)")
	modelName := flag.String("model", "hogweed", "Задается название модели для обработки. По умолчанию используется hogweed")
	modelPath := flag.String("model-path", "", "Необязательный явный путь к модели .onnx (переопределяет --model)")
	minArea := flag.Float64("min-area", 0, "Минимальная площадь полигона (в единицах CRS) для shp формата (0 - отключено)")
	device := flag.String("device", "cpu", "Тип расчетов: cpu|gpu")
	cudaID := flag.Int("cuda-device", 0, "Необязательный. Номер GPU, на котором нужно запускать ONNX Runtime (если задан --device=gpu)")
	batch := flag.Int("batch", 4, "Необязательный. Сколько тайлов обрабатывается одним вызовом ONNX Runtime")
	outPath := flag.String("output", "", "Сохраняемый с результатами путь к выходному Shapefile. Если не задан, сохраняем в result/<model>.shp")
	simplify := flag.Float64("simplify", 0, "Необязательный. Степень упрощения полигонов; 0 - без упрощения")
	modelsFile := flag.String("models-file", "models.json", "Путь к JSON-файлу с описаниями моделей")
	flag.Parse()

	// Загружаем описания моделей из внешнего файла (встроенных моделей больше нет).
	if *modelsFile == "" {
		fmt.Fprintln(os.Stderr, "Не указан путь к файлу моделей (--models-file)")
		os.Exit(2)
	}
	specs, err := LoadModelSpecsFromFile(*modelsFile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Ошибка загрузки файла моделей:", err)
		printJsonFormat()
		os.Exit(1)
	}
	SetModelSpecs(specs)

	if *inPath == "" {
		fmt.Fprintln(os.Stderr, "Аналитическая Система Информационного Мониторинга 2.0 (ASIM)\nГБУ ПК \"Центр информационного развития Пермского края\". 2026 год\n")
		fmt.Fprintln(os.Stderr, "./asim --input <файл.tif> --model <модель> --output <файл.shp> --device <cpu|gpu>\n\nИспользуйте --help для вывода всех параметров с описанием\n")
		names := ListModelNames()
		if len(names) == 0 {
			fmt.Println("Не найдены параметры моделей в файле", *modelsFile)
			printJsonFormat()
			return
		}
		fmt.Println("Список доступных моделей:")
		for _, name := range names {
			spec, ok := GetModelSpec(name)
			if !ok {
				continue
			}
			desc := strings.TrimSpace(spec.Description)
			if desc == "" {
				desc = "(описание не задано)"
			}
			fmt.Printf("\t%-17s %s\n", name, desc)
		}
		printJsonFormat()
		os.Exit(2)
	}

	dev := strings.ToLower(strings.TrimSpace(*device))
	if dev != "cpu" && dev != "gpu" {
		fmt.Println("Укажите параметр обработки --device: gpu - для работы с NVIDIA, cpu - для работы без NVIDIA")
		os.Exit(2)
	}
	if dev == "gpu" { dev = "cuda" }

	modelKey := strings.ToLower(strings.TrimSpace(*modelName))
	spec, ok := GetModelSpec(modelKey)
	if !ok {
		fmt.Fprintln(os.Stderr, "Неверное значение поля --model.")
		fmt.Fprintln(os.Stderr, "Доступные модели:", strings.Join(ListModelNames(), "|"))

		os.Exit(2)
	}

	// Если ONNXFile не задан в JSON, FinalizeSpec уже подставил <Name>.onnx
	onnx := *modelPath
	if onnx == "" {
		onnx = filepath.Join("models", spec.ONNXFile)
	}

	if *outPath == "" {
		_ = os.MkdirAll("result", 0o755)
		*outPath = filepath.Join("result", spec.Name+".shp")
	}

	if spec.Inputs > 1 && *inPath2 == "" {
		fmt.Fprintf(os.Stderr, "model %s требует второй вход (--input2), но он не указан\n", spec.Name)
		os.Exit(1)
	}

	effSimplify := *simplify
	if effSimplify == 0 && spec.Simplify > 0 {
		effSimplify = spec.Simplify
	}

	effSimplify = *simplify
	if effSimplify == 0 && spec.Simplify > 0 {
		effSimplify = spec.Simplify
	}

	if err := RunModel(
		*inPath,        // string: входной GeoTIFF
		onnx,           // string: путь к .onnx
		*outPath,       // string: куда писать результат (Shapefile)
		*batch,         // int: batchSize
		dev,            // string: "cpu" или "cuda"
		*cudaID,        // int: номер GPU
		*minArea,       // float64: минимальная площадь полигона
		effSimplify,    // float64: упрощение геометрии
		spec,           // ModelSpec: выбранная модель из models.json
	); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
