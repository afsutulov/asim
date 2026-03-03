package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func printJsonFormat() {
	fmt.Fprintln(os.Stderr, "\nФормат содержимого файла (пример):\n{\n\t\"hogweed\": {\n\t\t\"description\": \"Зарастание Борщевиком Сосновского\",\n\t\t\"onnx_file\": \"hogweed.onnx\",\"\n\t\t\"channels\": [\"B04\", \"B03\", \"B02\", \"B08\"],")
	fmt.Fprintln(os.Stderr, "\t\t\"tile\": 256,\n\t\t\"bound\": 32,\n\t\t\"threshold\": 0.6,\n\t\t\"divisor\": 10000,\n\t\t\"out_channels\": 1,\n\t\t\"mode\": \"binary\",\n\t\t\"preprocess\": \"sentinel\"\n\t}\n}")
}

func main() {
	inPath := flag.String("input", "", "Задается путь и имя GeoTIFF файла на обработку")
	inPath2 := flag.String("input2", "", "Опционально. Задается путь и имя второго GeoTIFF. Если модель должна сранивать два снимка: ОТ и ДО (значение поля \"input\" в описании моделе равно 2)")
	modelName := flag.String("model", "hogweed", "Задается название модели для обработки. По умолчанию используется hogweed")
	modelPath := flag.String("model-path", "", "Необязательный явный путь к модели .onnx (переопределяет --model)")
	outPath := flag.String("out", "", "Сохраняемый с результатом файл (tif для GeoTIFF, shp для Shapefile). Если не задан, сохраняем в result/<model>.(tif|shp)")
	format := flag.String("format", "shp", "Формат данных на выходе: tif|shp")
	device := flag.String("device", "cpu", "Тип расчетов: cpu|cuda|gpu")
	cudaID := flag.Int("cuda-device", 0, "Необязательный. Номер CUDA-устройства, на котором нужно запускать ONNX Runtime (если задан --device=cuda)")
	batch := flag.Int("batch", 4, "Необязательный. Сколько тайлов обрабатывается одним вызовом ONNX Runtime")
	minArea := flag.Float64("min-area", 0, "Минимальная площадь полигона (в единицах CRS) для shp формата (0 - отключено)")
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
		fmt.Fprintln(os.Stderr, "./asim --input <файл.tiff> --model <модель> --format <shp|tif> --out <файл.shp|.tiff> --device <cpu|cuda|gpu>\n\nИспользуйте --help для вывода всех параметров с описанием\n")
		names := ListModelNames()
		if len(names) == 0 {
			fmt.Println("Модели не найдены.\nФормат содержимого файла с моделями:\n\n\t\"hogweed\": \n\t\t\"description\": \"Зарастание Борщевиком Сосновского\",\n\nПроверьте содержимое файла", *modelsFile)
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
			fmt.Printf("\t%s\t\t%s\n", name, desc)
		}
		printJsonFormat()
		os.Exit(2)
	}

	dev := strings.ToLower(strings.TrimSpace(*device))
	if dev == "gpu" {
		dev = "cuda"
	}

	fmtStr := strings.ToLower(strings.TrimSpace(*format))
	if fmtStr != "tif" && fmtStr != "shp" {
		fmt.Fprintln(os.Stderr, "Неверное значение поля --format. Задайте tif или shp")
		os.Exit(2)
	}

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
		ext := ".shp"
		if fmtStr == "tif" {
			ext = ".tif"
		}
		*outPath = filepath.Join("result", spec.Name+ext)
	}

	if spec.Inputs > 1 && *inPath2 == "" {
		fmt.Fprintf(os.Stderr, "model %s требует второй вход (--input2), но он не указан\n", spec.Name)
		os.Exit(1)
	}

	if err := RunModel(
		*inPath,        // string: входной GeoTIFF
		onnx,           // string: путь к .onnx
		*outPath,       // string: куда писать результат
		*batch,         // int: batchSize
		dev,            // string: "cpu" или "gpu"
		*cudaID,        // int: номер GPU
		fmtStr,         // string: "tif" или "shp"
		*minArea,       // float64: минимальная площадь полигона
		*simplify,      // float64: упрощение геометрии
		spec,           // ModelSpec: выбранная модель из models.json
	); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}
