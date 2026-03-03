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

// PolygonizeMaskToShapefile converts a 1-band mask GeoTIFF (values 0/1) into a shapefile.
// It keeps only DN==1 polygons, optionally filtering by minArea (map units^2) and simplifying.
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
		// OSRImportFromWkt takes char**
		pwkt := wkt
		C.OSRImportFromWkt(srs, (**C.char)(unsafe.Pointer(&pwkt)))
	}

	layerName := C.CString("hogweed")
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
	fd := C.OGR_Fld_Create(C.CString("DN"), C.OFTInteger)
	if fd == nil {
		return errors.New("failed to create field def")
	}
	defer C.OGR_Fld_Destroy(fd)
	if C.OGR_L_CreateField(layer, fd, 1) != 0 {
		return errors.New("failed to create DN field")
	}
	fieldIndex := C.int(0)

	// Polygonize
	if C.GDALPolygonize(band, nil, layer, fieldIndex, nil, nil, nil) != 0 {
		return errors.New("GDALPolygonize failed")
	}

	// Post-process: keep DN==1, filter by area, simplify.
	C.OGR_L_ResetReading(layer)
	for {
		feat := C.OGR_L_GetNextFeature(layer)
		if feat == nil {
			break
		}
		fid := C.OGR_F_GetFID(feat)
		dn := C.OGR_F_GetFieldAsInteger(feat, fieldIndex)
		geom := C.OGR_F_GetGeometryRef(feat)
		remove := false
		if dn != 1 {
			remove = true
		} else if geom != nil && minArea > 0 {
			area := float64(C.OGR_G_Area(geom))
			if area < minArea {
				remove = true
			}
		}
		if remove {
			C.OGR_F_Destroy(feat)
			C.OGR_L_DeleteFeature(layer, fid)
			continue
		}
		if geom != nil && simplifyTol > 0 {
			newGeom := C.OGR_G_Simplify(geom, C.double(simplifyTol))
			if newGeom != nil {
				C.OGR_F_SetGeometryDirectly(feat, newGeom)
				C.OGR_L_SetFeature(layer, feat)
			}
		}
		C.OGR_F_Destroy(feat)
	}

	// Flush
	C.OGR_DS_SyncToDisk(ods)
	return nil
}
