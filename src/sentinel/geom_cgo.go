package main

/*
#cgo pkg-config: gdal
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "gdal.h"
#include "ogr_api.h"
#include "ogr_srs_api.h"

static void _gdal_init_geom_once() {
    GDALAllRegister();
    OGRRegisterAll();
}
static void _set_traditional_gis_order(OGRSpatialReferenceH srs) {
    if (srs != NULL) {
        OSRSetAxisMappingStrategy(srs, OAMS_TRADITIONAL_GIS_ORDER);
    }
}
static OGRGeometryH _empty_multi_polygon() { return OGR_G_CreateGeometry(wkbMultiPolygon); }
*/
import "C"

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unsafe"
)

type Geometry = C.OGRGeometryH

type Envelope struct {
	MinX float64
	MinY float64
	MaxX float64
	MaxY float64
}

func GeometryEnvelope(g Geometry) Envelope {
	var e Envelope
	if g == nil {
		return e
	}
	var env C.OGREnvelope
	C.OGR_G_GetEnvelope(g, &env)
	e.MinX = float64(env.MinX)
	e.MinY = float64(env.MinY)
	e.MaxX = float64(env.MaxX)
	e.MaxY = float64(env.MaxY)
	return e
}

func EnvelopeIntersects(a Envelope, b [4]float64) bool {
	return !(a.MaxX < b[0] || a.MinX > b[2] || a.MaxY < b[1] || a.MinY > b[3])
}

func initGeom() { C._gdal_init_geom_once() }
func DestroyGeometry(g Geometry) {
	if g != nil {
		C.OGR_G_DestroyGeometry(g)
	}
}

func cloneGeom(g Geometry) Geometry {
	if g == nil {
		return nil
	}
	return C.OGR_G_Clone(g)
}
func GeometryIntersects(a, b Geometry) bool {
	return a != nil && b != nil && C.OGR_G_Intersects(a, b) != 0
}
func GeometryIntersection(a, b Geometry) Geometry {
	if a == nil || b == nil {
		return nil
	}
	return C.OGR_G_Intersection(a, b)
}
func GeometryUnion(a, b Geometry) Geometry {
	if a == nil {
		return cloneGeom(b)
	}
	if b == nil {
		return cloneGeom(a)
	}
	return C.OGR_G_Union(a, b)
}
func GeometryDifference(a, b Geometry) Geometry {
	if a == nil {
		return nil
	}
	if b == nil {
		return cloneGeom(a)
	}
	return C.OGR_G_Difference(a, b)
}

func GeometryIsEmpty(g Geometry) bool {
	return g == nil || C.OGR_G_IsEmpty(g) != 0
}

func newEPSG4326SRS() C.OGRSpatialReferenceH {
	srs := C.OSRNewSpatialReference(nil)
	C.OSRImportFromEPSG(srs, 4326)
	C._set_traditional_gis_order(srs)
	return srs
}

func loadUnionGeometryWithSRS(path string) (Geometry, C.OGRSpatialReferenceH, error) {
	initGeom()
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	ds := C.GDALOpenEx(cp, C.GDAL_OF_VECTOR, nil, nil, nil)
	if ds == nil {
		return nil, nil, fmt.Errorf("open vector: %s", path)
	}
	defer C.GDALClose(ds)
	layer := C.GDALDatasetGetLayer(ds, 0)
	if layer == nil {
		return nil, nil, fmt.Errorf("layer 0 not found: %s", path)
	}
	var srs C.OGRSpatialReferenceH
	lsrs := C.OGR_L_GetSpatialRef(layer)
	if lsrs != nil {
		srs = C.OSRClone(lsrs)
		C._set_traditional_gis_order(srs)
	}
	C.OGR_L_ResetReading(layer)
	var acc Geometry
	for {
		feat := C.OGR_L_GetNextFeature(layer)
		if feat == nil {
			break
		}
		geom := C.OGR_F_GetGeometryRef(feat)
		if geom != nil {
			ug := GeometryUnion(acc, geom)
			if acc != nil {
				DestroyGeometry(acc)
			}
			acc = ug
		}
		C.OGR_F_Destroy(feat)
	}
	if acc == nil {
		acc = C._empty_multi_polygon()
	}
	return acc, srs, nil
}

func reprojectGeometryToWGS84(g Geometry, srcSRS C.OGRSpatialReferenceH) (Geometry, error) {
	if g == nil {
		return nil, nil
	}
	if srcSRS == nil {
		return cloneGeom(g), nil
	}
	C._set_traditional_gis_order(srcSRS)
	dst := newEPSG4326SRS()
	defer C.OSRDestroySpatialReference(dst)
	ct := C.OCTNewCoordinateTransformation(srcSRS, dst)
	if ct == nil {
		return nil, fmt.Errorf("create coordinate transformation to WGS84 failed")
	}
	defer C.OCTDestroyCoordinateTransformation(ct)
	cg := cloneGeom(g)
	if cg == nil {
		return nil, fmt.Errorf("clone geometry failed")
	}
	if C.OGR_G_Transform(cg, ct) != 0 {
		DestroyGeometry(cg)
		return nil, fmt.Errorf("geometry transform to WGS84 failed")
	}
	return cg, nil
}

func LoadUnionGeometryFromVector(path string) (Geometry, error) {
	g, srs, err := loadUnionGeometryWithSRS(path)
	if srs != nil {
		defer C.OSRDestroySpatialReference(srs)
	}
	if err != nil {
		return nil, err
	}
	wg, err := reprojectGeometryToWGS84(g, srs)
	DestroyGeometry(g)
	if err != nil {
		return nil, err
	}
	return wg, nil
}

func BuildAllowedGeometry(cfg *AppConfig, searchPolygon string, modelPolygons []string) (Geometry, error) {
	path, err := cfg.PolygonPath(searchPolygon)
	if err != nil {
		return nil, err
	}
	allowed, err := LoadUnionGeometryFromVector(path)
	if err != nil {
		return nil, err
	}
	for _, name := range modelPolygons {
		b, err := cfg.PolygonPath(name)
		if err != nil {
			DestroyGeometry(allowed)
			return nil, err
		}
		mg, err := LoadUnionGeometryFromVector(b)
		if err != nil {
			DestroyGeometry(allowed)
			return nil, err
		}
		inter := GeometryIntersection(allowed, mg)
		DestroyGeometry(allowed)
		DestroyGeometry(mg)
		allowed = inter
		if allowed == nil {
			allowed = C._empty_multi_polygon()
		}
	}
	return allowed, nil
}

func TileFootprintFromJP2(tile SafeTile, channels []string) (Geometry, error) {
	cube, err := ReadSentinelTileCube(tile, channels[:1])
	if err != nil {
		return nil, err
	}
	return RectFromGeo(cube.Geo, cube.W, cube.H), nil
}

func TileFootprintFromCache(tile SafeTile) Geometry {
	return RectFromEnvelope(tile.Envelope)
}

func RectFromEnvelope(env [4]float64) Geometry {
	initGeom()
	ring := C.OGR_G_CreateGeometry(C.wkbLinearRing)
	x0, y0 := env[0], env[1]
	x1, y1 := env[2], env[3]
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y0))
	C.OGR_G_AddPoint_2D(ring, C.double(x1), C.double(y0))
	C.OGR_G_AddPoint_2D(ring, C.double(x1), C.double(y1))
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y1))
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y0))
	poly := C.OGR_G_CreateGeometry(C.wkbPolygon)
	C.OGR_G_AddGeometryDirectly(poly, ring)
	return poly
}

func RectFromGeo(geo [6]float64, w, h int) Geometry {
	initGeom()
	ring := C.OGR_G_CreateGeometry(C.wkbLinearRing)
	x0, y0 := geo[0], geo[3]
	x1, y1 := geo[0]+float64(w)*geo[1]+float64(h)*geo[2], geo[3]+float64(w)*geo[4]+float64(h)*geo[5]
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y0))
	C.OGR_G_AddPoint_2D(ring, C.double(x1), C.double(y0))
	C.OGR_G_AddPoint_2D(ring, C.double(x1), C.double(y1))
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y1))
	C.OGR_G_AddPoint_2D(ring, C.double(x0), C.double(y0))
	poly := C.OGR_G_CreateGeometry(C.wkbPolygon)
	C.OGR_G_AddGeometryDirectly(poly, ring)
	return poly
}

func MergeShapefileIntoGeometry(shp string, allowedGeom Geometry, merged *Geometry) error {
	g, err := LoadUnionGeometryFromVector(shp)
	if err != nil {
		return err
	}
	defer DestroyGeometry(g)
	clip := GeometryIntersection(g, allowedGeom)
	if clip == nil {
		return nil
	}
	defer DestroyGeometry(clip)
	u := GeometryUnion(*merged, clip)
	if *merged != nil {
		DestroyGeometry(*merged)
	}
	*merged = u
	return nil
}

func GeometryCount(g Geometry) int {
	if g == nil {
		return 0
	}
	t := C.OGR_G_GetGeometryType(g)
	flat := C.OGR_GT_Flatten(t)
	if flat == C.wkbPolygon {
		return 1
	}
	if flat == C.wkbMultiPolygon || flat == C.wkbGeometryCollection {
		return int(C.OGR_G_GetGeometryCount(g))
	}
	return 1
}

func CleanupShapefileSet(shp string) {
	base := shp[:len(shp)-len(filepath.Ext(shp))]
	for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
		_ = os.Remove(base + ext)
	}
}

func WriteGeometryAsShapefile(g Geometry, outShp string) error {
	return writeGeometryToVector(g, outShp, "ESRI Shapefile")
}
func WriteGeometryAsGeoJSON(g Geometry, out string) error {
	return writeGeometryToVector(g, out, "GeoJSON")
}

func createEmptyVector(path string, driverName string) error {
	initGeom()
	base := filepath.Join(filepath.Dir(path), strings.TrimSuffix(filepath.Base(path), filepath.Ext(path)))
	if driverName == "ESRI Shapefile" {
		for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
			_ = os.Remove(base + ext)
		}
	} else {
		_ = os.Remove(path)
	}
	drvName := C.CString(driverName)
	defer C.free(unsafe.Pointer(drvName))
	drv := C.OGRGetDriverByName(drvName)
	if drv == nil {
		return fmt.Errorf("OGR driver not found: %s", driverName)
	}
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	ds := C.OGR_Dr_CreateDataSource(drv, cp, nil)
	if ds == nil {
		return fmt.Errorf("create vector: %s", path)
	}
	defer C.OGR_DS_Destroy(ds)
	layerName := C.CString("result")
	defer C.free(unsafe.Pointer(layerName))
	srs := newEPSG4326SRS()
	defer C.OSRDestroySpatialReference(srs)
	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, C.wkbMultiPolygon, nil)
	if layer == nil {
		return fmt.Errorf("create layer: %s", path)
	}
	C.OGR_DS_SyncToDisk(ds)
	return nil
}

func CreateEmptyShapefile(path string) error { return createEmptyVector(path, "ESRI Shapefile") }
func CreateEmptyGeoJSON(path string) error  { return createEmptyVector(path, "GeoJSON") }

func openLayerForAppend(path string) (C.GDALDatasetH, C.OGRLayerH, error) {
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	ds := C.GDALOpenEx(cp, C.GDAL_OF_VECTOR|C.GDAL_OF_UPDATE, nil, nil, nil)
	if ds == nil {
		return nil, nil, fmt.Errorf("open vector for update: %s", path)
	}
	layer := C.GDALDatasetGetLayer(ds, 0)
	if layer == nil {
		C.GDALClose(ds)
		return nil, nil, fmt.Errorf("layer 0 not found: %s", path)
	}
	return ds, layer, nil
}

func AppendShapefileFeaturesClipped(srcShp string, allowedGeom Geometry, dstShp string) error {
	initGeom()
	srcCP := C.CString(srcShp)
	defer C.free(unsafe.Pointer(srcCP))
	srcDS := C.GDALOpenEx(srcCP, C.GDAL_OF_VECTOR, nil, nil, nil)
	if srcDS == nil {
		return fmt.Errorf("open source vector: %s", srcShp)
	}
	defer C.GDALClose(srcDS)
	srcLayer := C.GDALDatasetGetLayer(srcDS, 0)
	if srcLayer == nil {
		return fmt.Errorf("source layer not found: %s", srcShp)
	}
	if _, err := os.Stat(dstShp); err != nil {
		if err := CreateEmptyShapefile(dstShp); err != nil {
			return err
		}
	}
	dstDS, dstLayer, err := openLayerForAppend(dstShp)
	if err != nil {
		return err
	}
	defer C.GDALClose(dstDS)
	dstDefn := C.OGR_L_GetLayerDefn(dstLayer)

	var srcSRS C.OGRSpatialReferenceH
	lsrs := C.OGR_L_GetSpatialRef(srcLayer)
	if lsrs != nil {
		srcSRS = C.OSRClone(lsrs)
		C._set_traditional_gis_order(srcSRS)
		defer C.OSRDestroySpatialReference(srcSRS)
	}

	C.OGR_L_ResetReading(srcLayer)
	for {
		feat := C.OGR_L_GetNextFeature(srcLayer)
		if feat == nil {
			break
		}
		geom := C.OGR_F_GetGeometryRef(feat)
		if geom == nil {
			C.OGR_F_Destroy(feat)
			continue
		}
		wg, err := reprojectGeometryToWGS84(geom, srcSRS)
		if err != nil || wg == nil {
			if wg != nil {
				DestroyGeometry(wg)
			}
			C.OGR_F_Destroy(feat)
			continue
		}
		clip := GeometryIntersection(wg, allowedGeom)
		DestroyGeometry(wg)
		if clip != nil && !GeometryIsEmpty(clip) {
			outFeat := C.OGR_F_Create(dstDefn)
			if outFeat != nil {
				if C.OGR_F_SetGeometry(outFeat, clip) == 0 {
					C.OGR_L_CreateFeature(dstLayer, outFeat)
				}
				C.OGR_F_Destroy(outFeat)
			}
		}
		if clip != nil {
			DestroyGeometry(clip)
		}
		C.OGR_F_Destroy(feat)
	}
	C.GDALFlushCache(dstDS)
	return nil
}

func CountFeaturesInVector(path string) (int, error) {
	initGeom()
	cp := C.CString(path)
	defer C.free(unsafe.Pointer(cp))
	ds := C.GDALOpenEx(cp, C.GDAL_OF_VECTOR, nil, nil, nil)
	if ds == nil {
		return 0, fmt.Errorf("open vector: %s", path)
	}
	defer C.GDALClose(ds)
	layer := C.GDALDatasetGetLayer(ds, 0)
	if layer == nil {
		return 0, fmt.Errorf("layer 0 not found: %s", path)
	}
	return int(C.OGR_L_GetFeatureCount(layer, 1)), nil
}

func ConvertShapefileToGeoJSON(srcShp string, outGeoJSON string) error {
	if err := CreateEmptyGeoJSON(outGeoJSON); err != nil {
		return err
	}
	world := RectFromEnvelope([4]float64{-180, -90, 180, 90})
	defer DestroyGeometry(world)
	return AppendShapefileFeaturesClipped(srcShp, world, outGeoJSON)
}

func writeGeometryToVector(g Geometry, outPath, driverName string) error {
	initGeom()
	cd := C.CString(driverName)
	defer C.free(unsafe.Pointer(cd))
	drv := C.OGRGetDriverByName(cd)
	if drv == nil {
		return fmt.Errorf("driver not found: %s", driverName)
	}
	cp := C.CString(outPath)
	defer C.free(unsafe.Pointer(cp))
	ds := C.OGR_Dr_CreateDataSource(drv, cp, nil)
	if ds == nil {
		return fmt.Errorf("create vector: %s", outPath)
	}
	defer C.OGR_DS_Destroy(ds)
	layerName := C.CString("result")
	defer C.free(unsafe.Pointer(layerName))
	srs := newEPSG4326SRS()
	defer C.OSRDestroySpatialReference(srs)
	layer := C.OGR_DS_CreateLayer(ds, layerName, srs, C.wkbMultiPolygon, nil)
	if layer == nil {
		return fmt.Errorf("create layer: %s", outPath)
	}
	feat := C.OGR_F_Create(C.OGR_L_GetLayerDefn(layer))
	if feat == nil {
		return fmt.Errorf("create feature: %s", outPath)
	}
	defer C.OGR_F_Destroy(feat)
	if C.OGR_F_SetGeometry(feat, g) != 0 {
		return fmt.Errorf("set geometry: %s", outPath)
	}
	if C.OGR_L_CreateFeature(layer, feat) != 0 {
		return fmt.Errorf("write feature: %s", outPath)
	}
	C.OGR_DS_SyncToDisk(ds)
	return nil
}
