package main

/*
#cgo pkg-config: gdal
#include "gdal.h"
#include "gdal_alg.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"
#include "cpl_conv.h"
#include <stdlib.h>

static void _gdal_init_once_merge() {
    GDALAllRegister();
    OGRRegisterAll();
}
*/
import "C"

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

func pixelSizeForSpec(spec ModelSpec) float64 {
	if strings.EqualFold(spec.Preprocess, "sentinel2a") {
		switch strings.ToUpper(strings.TrimSpace(spec.Resolution)) {
		case "R20M":
			return 20
		case "R60M":
			return 60
		default:
			return 10
		}
	}
	return 10
}

func sieveThresholdPixels(spec ModelSpec) int {
	px := pixelSizeForSpec(spec)
	if spec.MinArea <= 0 {
		return 8
	}
	n := int(math.Ceil(spec.MinArea / (px * px)))
	if n < 8 {
		n = 8
	}
	return n
}

func SieveMergedShapefile(srcShp, dstShp string, spec ModelSpec) error {
	C._gdal_init_once_merge()
	pxSize := pixelSizeForSpec(spec)
	threshold := sieveThresholdPixels(spec)

	srcC := C.CString(srcShp)
	defer C.free(unsafe.Pointer(srcC))
	srcDS := C.GDALOpenEx(srcC, C.GDAL_OF_VECTOR, nil, nil, nil)
	if srcDS == nil {
		return fmt.Errorf("open vector: %s", srcShp)
	}
	defer C.GDALClose(srcDS)
	srcLayer := C.GDALDatasetGetLayer(srcDS, 0)
	if srcLayer == nil {
		return fmt.Errorf("layer 0 not found: %s", srcShp)
	}

	// Берём extent в WGS84 и работаем в WebMercator, чтобы размер пикселя был в метрах.
	var ext C.OGREnvelope
	if C.OGR_L_GetExtent(srcLayer, &ext, 1) != 0 {
		return fmt.Errorf("extent failed: %s", srcShp)
	}
	minx, miny, maxx, maxy := float64(ext.MinX), float64(ext.MinY), float64(ext.MaxX), float64(ext.MaxY)
	if maxx <= minx || maxy <= miny {
		return fmt.Errorf("empty extent: %s", srcShp)
	}

	// Грубое преобразование градусов в метры через EPSG:3857 размером пикселя pxSize.
	originShift := 20037508.342789244
	lonToMerc := func(lon float64) float64 { return lon * originShift / 180.0 }
	latToMerc := func(lat float64) float64 {
		if lat > 89.5 {
			lat = 89.5
		}
		if lat < -89.5 {
			lat = -89.5
		}
		return math.Log(math.Tan((90.0+lat)*math.Pi/360.0)) * originShift / math.Pi
	}
	mx0, my0 := lonToMerc(minx), latToMerc(miny)
	mx1, my1 := lonToMerc(maxx), latToMerc(maxy)
	w := int(math.Ceil((mx1 - mx0) / pxSize))
	h := int(math.Ceil((my1 - my0) / pxSize))
	if w < 1 || h < 1 {
		return fmt.Errorf("invalid raster size for sieve: %dx%d", w, h)
	}

	drvName := C.CString("MEM")
	defer C.free(unsafe.Pointer(drvName))
	drv := C.GDALGetDriverByName(drvName)
	if drv == nil {
		return fmt.Errorf("MEM driver not found")
	}
	tmpName := C.CString("")
	defer C.free(unsafe.Pointer(tmpName))
	rasterDS := C.GDALCreate(drv, tmpName, C.int(w), C.int(h), 1, C.GDT_Byte, nil)
	if rasterDS == nil {
		return fmt.Errorf("create in-memory raster failed")
	}
	defer C.GDALClose(rasterDS)
	gt := []C.double{C.double(mx0), C.double(pxSize), 0, C.double(my1), 0, C.double(-pxSize)}
	C.GDALSetGeoTransform(rasterDS, &gt[0])
	wm := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(wm)
	C.OSRImportFromEPSG(wm, 3857)
	var wkt *C.char
	C.OSRExportToWkt(wm, &wkt)
	if wkt != nil {
		C.GDALSetProjection(rasterDS, wkt)
		C.CPLFree(unsafe.Pointer(wkt))
	}

	band := C.GDALGetRasterBand(rasterDS, 1)
	if band == nil {
		return fmt.Errorf("raster band missing")
	}
	bandList := []C.int{1}
	burn := []C.double{1}
	layers := []C.OGRLayerH{srcLayer}
	if C.GDALRasterizeLayers(rasterDS, 1, &bandList[0], 1, &layers[0], nil, nil, &burn[0], nil, nil, nil) != 0 {
		return fmt.Errorf("GDALRasterizeLayers failed")
	}

	sievedDS := C.GDALCreate(drv, tmpName, C.int(w), C.int(h), 1, C.GDT_Byte, nil)
	if sievedDS == nil {
		return fmt.Errorf("create sieve raster failed")
	}
	defer C.GDALClose(sievedDS)
	C.GDALSetGeoTransform(sievedDS, &gt[0])
	var wkt2 *C.char
	C.OSRExportToWkt(wm, &wkt2)
	if wkt2 != nil {
		C.GDALSetProjection(sievedDS, wkt2)
		C.CPLFree(unsafe.Pointer(wkt2))
	}
	sievedBand := C.GDALGetRasterBand(sievedDS, 1)
	if sievedBand == nil {
		return fmt.Errorf("sieve raster band missing")
	}
	if C.GDALSieveFilter(band, nil, sievedBand, C.int(threshold), 8, nil, nil, nil) != 0 {
		return fmt.Errorf("GDALSieveFilter failed")
	}

	tmpTif := strings.TrimSuffix(dstShp, filepath.Ext(dstShp)) + ".tmp.tif"
	gtiffName := C.CString("GTiff")
	defer C.free(unsafe.Pointer(gtiffName))
	gtiff := C.GDALGetDriverByName(gtiffName)
	if gtiff == nil {
		return DissolveOverlappingPolygons(srcShp, dstShp)
	}
	outTifC := C.CString(tmpTif)
	defer C.free(unsafe.Pointer(outTifC))
	copyDS := C.GDALCreateCopy(gtiff, outTifC, sievedDS, 0, nil, nil, nil)
	if copyDS == nil {
		return DissolveOverlappingPolygons(srcShp, dstShp)
	}
	C.GDALClose(copyDS)
	defer func() { _ = os.Remove(tmpTif) }()
	polyShp := strings.TrimSuffix(dstShp, filepath.Ext(dstShp)) + ".tmp_poly.shp"
	if err := PolygonizeMaskToShapefile(tmpTif, polyShp, spec.MinArea, spec.Simplify); err != nil {
		return DissolveOverlappingPolygons(srcShp, dstShp)
	}
	defer CleanupShapefileSet(polyShp)
	return DissolveOverlappingPolygons(polyShp, dstShp)
}
