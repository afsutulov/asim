package main

// Входной GeoTIFF ожидается с 13 каналами Sentinel-2 в строгом порядке:
//
//	0 B04 (Red)
//	1 B03 (Green)
//	2 B02 (Blue)
//	3 B01 (Coastal aerosol)
//	4 B05 (Red edge 1)
//	5 B06 (Red edge 2)
//	6 B07 (Red edge 3)
//	7 B08 (NIR)
//	8 B09 (water vapour)
//	9 B10 (cirrus)
//	10 B11 (SWIR1)
//	11 B12 (SWIR2)
//	12 B8A (NIR narrow)
//
// Этот порядок должен соответствовать порядку слоёв во входном GeoTIFF.
type Band int

const (
	B04 Band = iota
	B03
	B02
	B01
	B05
	B06
	B07
	B08
	B09
	B10
	B11
	B12
	B8A
)

// BandNameToIndex возвращает индекс канала в нашем 13-канальном GeoTIFF по имени полосы Sentinel-2 (например "B04").
// Возвращает (idx, true) если полоса поддерживается, иначе (0, false).
func BandNameToIndex(name string) (int, bool) {
	switch name {
	case "B04":
		return int(B04), true
	case "B03":
		return int(B03), true
	case "B02":
		return int(B02), true
	case "B01":
		return int(B01), true
	case "B05":
		return int(B05), true
	case "B06":
		return int(B06), true
	case "B07":
		return int(B07), true
	case "B08":
		return int(B08), true
	case "B09":
		return int(B09), true
	case "B10":
		return int(B10), true
	case "B11":
		return int(B11), true
	case "B12":
		return int(B12), true
	case "B8A":
		return int(B8A), true

	// алиасы RGB
	case "RED":
		return int(B04), true
	case "GRN":
		return int(B03), true
	case "BLU":
		return int(B02), true
	default:
		return 0, false
	}
}
