package main

/*
#cgo pkg-config: gdal

#include <stdio.h>

#include "gdal.h"
#include "gdal_alg.h"
#include "cpl_conv.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"

static void _gdal_init_once() {
    GDALAllRegister();
    OGRRegisterAll();
}

static int _file_exists(const char* path) {
    FILE* f = fopen(path, "rb");
    if (!f) return 0;
    fclose(f);
    return 1;
}

static void _try_remove(const char* path) {
    remove(path);
}

*/
import "C"

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"
)

// PolygonizeMaskToShapefile converts a 1-band mask GeoTIFF into a shapefile.
// It keeps only DN > 0 polygons, optionally filtering by minArea and simplifying.
func PolygonizeMaskToShapefile(maskTif string, outShp string, minArea float64, simplifyTol float64) error {
	C._gdal_init_once()

	cmask := C.CString(maskTif)
	defer C.free(unsafe.Pointer(cmask))

	ds := C.GDALOpen(cmask, C.GA_ReadOnly)
	if ds == nil {
		return fmt.Errorf("gdal open mask: %s", maskTif)
	}
	defer C.GDALClose(ds)

	band := C.GDALGetRasterBand(ds, 1)
	if band == nil {
		return errors.New("mask has no band 1")
	}

	// Remove existing shapefile set if present.
	base := strings.TrimSuffix(outShp, filepath.Ext(outShp))
	for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
		p := base + ext
		cp := C.CString(p)
		if C._file_exists(cp) != 0 {
			C._try_remove(cp)
		}
		C.free(unsafe.Pointer(cp))
	}

	// Create output datasource
	drvName := C.CString("ESRI Shapefile")
	defer C.free(unsafe.Pointer(drvName))
	drv := C.OGRGetDriverByName(drvName)
	if drv == nil {
		return errors.New("OGR driver ESRI Shapefile not found")
	}

	outPathC := C.CString(outShp)
	defer C.free(unsafe.Pointer(outPathC))
	ods := C.OGR_Dr_CreateDataSource(drv, outPathC, nil)
	if ods == nil {
		return fmt.Errorf("create shp: %s", outShp)
	}
	defer C.OGR_DS_Destroy(ods)

	// Spatial reference from raster projection
	proj := C.GDALGetProjectionRef(ds)
	var srs C.OGRSpatialReferenceH
	if proj != nil && *proj != 0 {
		srs = C.OSRNewSpatialReference(nil)
		wkt := C.CString(C.GoString(proj))
		defer C.free(unsafe.Pointer(wkt))
		pwkt := wkt
		C.OSRImportFromWkt(srs, (**C.char)(unsafe.Pointer(&pwkt)))
	}

	layerName := C.CString("result")
	defer C.free(unsafe.Pointer(layerName))
	layer := C.OGR_DS_CreateLayer(ods, layerName, srs, C.wkbPolygon, nil)
	if layer == nil {
		if srs != nil {
			C.OSRDestroySpatialReference(srs)
		}
		return errors.New("failed to create layer")
	}
	if srs != nil {
		C.OSRDestroySpatialReference(srs)
	}

	// Field DN (integer)
	fieldName := C.CString("DN")
	defer C.free(unsafe.Pointer(fieldName))
	fd := C.OGR_Fld_Create(fieldName, C.OFTInteger)
	if fd == nil {
		return errors.New("failed to create field def")
	}
	defer C.OGR_Fld_Destroy(fd)
	if C.OGR_L_CreateField(layer, fd, 1) != 0 {
		return errors.New("failed to create DN field")
	}

	// Polygonize во временный in-memory слой.
	memDrvName := C.CString("Memory")
	defer C.free(unsafe.Pointer(memDrvName))
	memDrv := C.OGRGetDriverByName(memDrvName)
	if memDrv == nil {
		return errors.New("OGR Memory driver not found")
	}

	memDSName := C.CString("tmp_poly")
	defer C.free(unsafe.Pointer(memDSName))
	memDS := C.OGR_Dr_CreateDataSource(memDrv, memDSName, nil)
	if memDS == nil {
		return errors.New("failed to create in-memory datasource")
	}
	defer C.OGR_DS_Destroy(memDS)

	tmpLayerName := C.CString("poly")
	defer C.free(unsafe.Pointer(tmpLayerName))
	tmpLayer := C.OGR_DS_CreateLayer(memDS, tmpLayerName, nil, C.wkbPolygon, nil)
	if tmpLayer == nil {
		return errors.New("failed to create temp layer")
	}

	tmpFieldName := C.CString("DN")
	defer C.free(unsafe.Pointer(tmpFieldName))
	tmpFd := C.OGR_Fld_Create(tmpFieldName, C.OFTInteger)
	if tmpFd == nil {
		return errors.New("failed to create temp DN field")
	}
	defer C.OGR_Fld_Destroy(tmpFd)

	if C.OGR_L_CreateField(tmpLayer, tmpFd, 1) != 0 {
		return errors.New("failed to create temp DN field")
	}
	tmpFieldIndex := C.int(0)

	if C.GDALPolygonize(band, nil, tmpLayer, tmpFieldIndex, nil, nil, nil) != 0 {
		return errors.New("GDALPolygonize failed")
	}

	// Копируем только DN > 0 с фильтрацией площади и simplify.
	outDefn := C.OGR_L_GetLayerDefn(layer)
	C.OGR_L_ResetReading(tmpLayer)

	for {
		feat := C.OGR_L_GetNextFeature(tmpLayer)
		if feat == nil {
			break
		}

		dn := int(C.OGR_F_GetFieldAsInteger(feat, tmpFieldIndex))
		if dn <= 0 {
			C.OGR_F_Destroy(feat)
			continue
		}

		geom := C.OGR_F_GetGeometryRef(feat)
		if geom == nil {
			C.OGR_F_Destroy(feat)
			continue
		}

		if minArea > 0 {
			area := float64(C.OGR_G_Area(geom))
			if area < minArea {
				C.OGR_F_Destroy(feat)
				continue
			}
		}

		var outGeom C.OGRGeometryH = geom
		if simplifyTol > 0 {
			newGeom := C.OGR_G_SimplifyPreserveTopology(geom, C.double(simplifyTol))
			if newGeom != nil {
				outGeom = newGeom
			}
		}

		outFeat := C.OGR_F_Create(outDefn)
		if outFeat != nil {
			C.OGR_F_SetFieldInteger(outFeat, 0, C.int(dn))
			C.OGR_F_SetGeometry(outFeat, outGeom)
			C.OGR_L_CreateFeature(layer, outFeat)
			C.OGR_F_Destroy(outFeat)
		}

		if outGeom != geom {
			C.OGR_G_DestroyGeometry(outGeom)
		}
		C.OGR_F_Destroy(feat)
	}

	C.OGR_DS_SyncToDisk(ods)
	return nil
}

// PolygonizeMask converts a temporary mask raster into a temporary shapefile and returns its .shp path.
func PolygonizeMask(maskTif string, classValue int, minArea float64, simplifyTol float64) (string, error) {
	_ = classValue
	shp := strings.TrimSuffix(maskTif, filepath.Ext(maskTif)) + ".shp"
	if err := PolygonizeMaskToShapefile(maskTif, shp, minArea, simplifyTol); err != nil {
		return "", err
	}
	return shp, nil
}
