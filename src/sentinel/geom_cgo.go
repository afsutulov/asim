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

func BuildAllowedGeometry(cfg *AppConfig, searchPolygon string, poligonsOn []string, poligonsOff []string) (Geometry, error) {
    path, err := cfg.PolygonPath(searchPolygon)
    if err != nil {
    return nil, err
    }
    allowed, err := LoadUnionGeometryFromVector(path)
    if err != nil {
    return nil, err
    }
    // poligons-on: пересечение с каждым — результат должен попадать в них.
    for _, name := range poligonsOn {
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
    // poligons-off: вычитание — результат не должен попадать в них.
    for _, name := range poligonsOff {
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
    diff := GeometryDifference(allowed, mg)
    DestroyGeometry(allowed)
    DestroyGeometry(mg)
    allowed = diff
    if allowed == nil {
        allowed = C._empty_multi_polygon()
    }
    }
    return allowed, nil
}

func TileFootprintFromJP2(tile SafeTile, channels []string) (Geometry, error) {
    cube, err := ReadSentinelTileCube(tile, channels[:1], "none")
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
    // Используем wkbMultiPolygon: все геометрии нормализуются до MultiPolygon
    // через toMultiPolygon() перед записью, поэтому тип всегда совпадает.
    // wkbUnknown нельзя использовать в Shapefile — GDAL записывает shapeType=0 (Null),
    // и при чтении все геометрии возвращаются как NULL → данные теряются.
    layer := C.OGR_DS_CreateLayer(ds, layerName, srs, C.wkbMultiPolygon, nil)
    if layer == nil {
    return fmt.Errorf("create layer: %s", path)
    }
    fdName := C.CString("DN")
    fd := C.OGR_Fld_Create(fdName, C.OFTInteger)
    C.free(unsafe.Pointer(fdName))
    if fd != nil {
    C.OGR_L_CreateField(layer, fd, 1)
    C.OGR_Fld_Destroy(fd)
    }
    C.OGR_DS_SyncToDisk(ds)
    return nil
}

func CreateEmptyShapefile(path string) error { return createEmptyVector(path, "ESRI Shapefile") }
func CreateEmptyGeoJSON(path string) error   { return createEmptyVector(path, "GeoJSON") }

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


// toMultiPolygon нормализует любую геометрию к типу MultiPolygon.
// Если g уже MultiPolygon — возвращает g без изменений (владение не передаётся).
// Если g является Polygon — создаёт новый MultiPolygon и добавляет клон g.
// Если g является GeometryCollection — собирает все полигоны внутри в один MultiPolygon.
// Во всех остальных случаях возвращает g как есть.
// Вызывающий код обязан уничтожить возвращённую геометрию если она != g.
func toMultiPolygon(g C.OGRGeometryH) C.OGRGeometryH {
    if g == nil {
    return nil
    }
    flat := C.OGR_GT_Flatten(C.OGR_G_GetGeometryType(g))
    if flat == C.wkbMultiPolygon {
    return g // уже нужный тип
    }
    mp := C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
    if flat == C.wkbPolygon {
    C.OGR_G_AddGeometry(mp, g) // AddGeometry копирует
    return mp
    }
    if flat == C.wkbGeometryCollection || flat == C.wkbMultiPolygon {
    n := int(C.OGR_G_GetGeometryCount(g))
    for i := 0; i < n; i++ {
        sub := C.OGR_G_GetGeometryRef(g, C.int(i))
        subFlat := C.OGR_GT_Flatten(C.OGR_G_GetGeometryType(sub))
        if subFlat == C.wkbPolygon {
	C.OGR_G_AddGeometry(mp, sub)
        }
    }
    if C.OGR_G_GetGeometryCount(mp) > 0 {
        return mp
    }
    C.OGR_G_DestroyGeometry(mp)
    return g
    }
    C.OGR_G_DestroyGeometry(mp)
    return g
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
    dstDNIdx := C.OGR_FD_GetFieldIndex(dstDefn, C.CString("DN"))

    var srcSRS C.OGRSpatialReferenceH
    lsrs := C.OGR_L_GetSpatialRef(srcLayer)
    if lsrs != nil {
    srcSRS = C.OSRClone(lsrs)
    C._set_traditional_gis_order(srcSRS)
    defer C.OSRDestroySpatialReference(srcSRS)
    }

    srcDefn := C.OGR_L_GetLayerDefn(srcLayer)
    srcDNIdx := C.OGR_FD_GetFieldIndex(srcDefn, C.CString("DN"))
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
        // Нормализуем геометрию до MultiPolygon: одиночный Polygon оборачиваем,
        // GeometryCollection конвертируем. Это гарантирует единый тип в выходном
        // файле, что важно для корректного отображения в ГИС-системах (заливка).
        normalized := toMultiPolygon(clip)
        if normalized != clip {
	DestroyGeometry(clip)
	clip = normalized
        }
        outFeat := C.OGR_F_Create(dstDefn)
        if outFeat != nil {
	if C.OGR_F_SetGeometry(outFeat, clip) == 0 {
	    if srcDNIdx >= 0 && dstDNIdx >= 0 {
	    dn := C.OGR_F_GetFieldAsInteger(feat, srcDNIdx)
	    C.OGR_F_SetFieldInteger(outFeat, dstDNIdx, dn)
	    }
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
    if err := CreateEmptyGeoJSON(outGeoJSON); err != nil {
    return err
    }
    dstDS, dstLayer, err := openLayerForAppend(outGeoJSON)
    if err != nil {
    return err
    }
    defer C.GDALClose(dstDS)
    dstDefn := C.OGR_L_GetLayerDefn(dstLayer)
    fdDN := C.CString("DN")
    defer C.free(unsafe.Pointer(fdDN))
    srcDNIndex := C.OGR_FD_GetFieldIndex(C.OGR_L_GetLayerDefn(srcLayer), fdDN)
    dstDNIndex := C.OGR_FD_GetFieldIndex(dstDefn, fdDN)
    var srcSRS C.OGRSpatialReferenceH
    if lsrs := C.OGR_L_GetSpatialRef(srcLayer); lsrs != nil {
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
    if err != nil || wg == nil || GeometryIsEmpty(wg) {
        if wg != nil {
	DestroyGeometry(wg)
        }
        C.OGR_F_Destroy(feat)
        continue
    }
    outFeat := C.OGR_F_Create(dstDefn)
    if outFeat != nil {
        if C.OGR_F_SetGeometry(outFeat, wg) == 0 {
	if srcDNIndex >= 0 && dstDNIndex >= 0 {
	    C.OGR_F_SetFieldInteger(outFeat, dstDNIndex, C.OGR_F_GetFieldAsInteger(feat, srcDNIndex))
	}
	C.OGR_L_CreateFeature(dstLayer, outFeat)
        }
        C.OGR_F_Destroy(outFeat)
    }
    DestroyGeometry(wg)
    C.OGR_F_Destroy(feat)
    }
    C.GDALFlushCache(dstDS)
    return nil
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

// DissolveOverlappingPolygons читает все полигоны из srcShp, объединяет
// перекрывающиеся (union), и записывает результат в dstShp.
// Это устраняет дублирование полигонов на стыках Sentinel-сцен.
//
// Алгоритм: собираем все подполигоны в MultiPolygon по классу DN,
// затем вызываем OGR_G_UnionCascaded (= GEOSUnaryUnion) — одну операцию
// вместо N инкрементальных. Это на порядки быстрее при N > 1000 полигонов.
// Важно: OGR_G_UnionCascaded принимает строго wkbMultiPolygon.
func DissolveOverlappingPolygons(srcShp, dstShp string) error {
    initGeom()

    srcCP := C.CString(srcShp)
    defer C.free(unsafe.Pointer(srcCP))
    srcDS := C.GDALOpenEx(srcCP, C.GDAL_OF_VECTOR, nil, nil, nil)
    if srcDS == nil {
    return fmt.Errorf("dissolve: open source: %s", srcShp)
    }
    defer C.GDALClose(srcDS)

    srcLayer := C.GDALDatasetGetLayer(srcDS, 0)
    if srcLayer == nil {
    return fmt.Errorf("dissolve: layer not found: %s", srcShp)
    }

    // Собираем все полигоны в MultiPolygon по классу DN.
    // OGR_G_UnionCascaded требует строго wkbMultiPolygon (не GeometryCollection) —
    // иначе GEOS выбрасывает "Invalid argument (must be a MultiPolygon)".
    // Все входящие геометрии (Polygon / MultiPolygon) раскладываем в плоский
    // список подполигонов и добавляем в MultiPolygon через OGR_G_AddGeometry (копирует).
    srcDefn := C.OGR_L_GetLayerDefn(srcLayer)
    dnIdx := C.OGR_FD_GetFieldIndex(srcDefn, C.CString("DN"))

    collByDN := map[int]C.OGRGeometryH{}
    defer func() {
    for _, coll := range collByDN {
        if coll != nil {
	C.OGR_G_DestroyGeometry(coll)
        }
    }
    }()

    // addPolyToMulti добавляет все подполигоны из g в mp (OGR_G_AddGeometry копирует).
    addPolyToMulti := func(mp, g C.OGRGeometryH) {
    if g == nil {
        return
    }
    flat := C.OGR_GT_Flatten(C.OGR_G_GetGeometryType(g))
    if flat == C.wkbPolygon {
        C.OGR_G_AddGeometry(mp, g)
        return
    }
    if flat == C.wkbMultiPolygon || flat == C.wkbGeometryCollection {
        n := int(C.OGR_G_GetGeometryCount(g))
        for i := 0; i < n; i++ {
	sub := C.OGR_G_GetGeometryRef(g, C.int(i))
	subFlat := C.OGR_GT_Flatten(C.OGR_G_GetGeometryType(sub))
	if subFlat == C.wkbPolygon {
	    C.OGR_G_AddGeometry(mp, sub)
	}
        }
    }
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
    dn := 1
    if dnIdx >= 0 {
        dn = int(C.OGR_F_GetFieldAsInteger(feat, dnIdx))
        if dn <= 0 {
	dn = 1
        }
    }
    if _, ok := collByDN[dn]; !ok {
        collByDN[dn] = C.OGR_G_CreateGeometry(C.wkbMultiPolygon)
    }
    // OGR_G_AddGeometry копирует геометрию — geom остаётся во владении feat
    addPolyToMulti(collByDN[dn], geom)
    C.OGR_F_Destroy(feat)
    }

    // Удаляем существующий файл если есть.
    base := strings.TrimSuffix(dstShp, filepath.Ext(dstShp))
    for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
    _ = os.Remove(base + ext)
    }

    drvName := C.CString("ESRI Shapefile")
    defer C.free(unsafe.Pointer(drvName))
    drv := C.OGRGetDriverByName(drvName)
    if drv == nil {
    return fmt.Errorf("dissolve: ESRI Shapefile driver not found")
    }
    dstCP := C.CString(dstShp)
    defer C.free(unsafe.Pointer(dstCP))
    dstDS := C.OGR_Dr_CreateDataSource(drv, dstCP, nil)
    if dstDS == nil {
    return fmt.Errorf("dissolve: create dst: %s", dstShp)
    }
    defer C.OGR_DS_Destroy(dstDS)

    // Копируем SRS из источника.
    var srs C.OGRSpatialReferenceH
    lsrs := C.OGR_L_GetSpatialRef(srcLayer)
    if lsrs != nil {
    srs = C.OSRClone(lsrs)
    defer C.OSRDestroySpatialReference(srs)
    }
    layerName := C.CString("dissolved")
    defer C.free(unsafe.Pointer(layerName))
    dstLayer := C.OGR_DS_CreateLayer(dstDS, layerName, srs, C.wkbMultiPolygon, nil)
    if dstLayer == nil {
    return fmt.Errorf("dissolve: create layer: %s", dstShp)
    }
    // Добавляем поле DN для сохранения класса (важно для многоклассовых моделей).
    dstDNFieldName := C.CString("DN")
    dstDNFieldDef := C.OGR_Fld_Create(dstDNFieldName, C.OFTInteger)
    C.free(unsafe.Pointer(dstDNFieldName))
    if dstDNFieldDef != nil {
    C.OGR_L_CreateField(dstLayer, dstDNFieldDef, 1)
    C.OGR_Fld_Destroy(dstDNFieldDef)
    }
    dstDefn := C.OGR_L_GetLayerDefn(dstLayer)
    dstDNIdx := C.OGR_FD_GetFieldIndex(dstDefn, C.CString("DN"))

    writeGeom := func(g C.OGRGeometryH, dn int) {
    if g == nil || C.OGR_G_IsEmpty(g) != 0 {
        return
    }
    // Нормализуем до MultiPolygon и пишем одной фичей.
    // Это гарантирует что все результаты в итоговом шейпфайле имеют
    // единый тип MultiPolygon — ГИС-системы отображают их с заливкой.
    mp := toMultiPolygon(g)
    f := C.OGR_F_Create(dstDefn)
    if f != nil {
        C.OGR_F_SetGeometry(f, mp)
        if dstDNIdx >= 0 {
	C.OGR_F_SetFieldInteger(f, dstDNIdx, C.int(dn))
        }
        C.OGR_L_CreateFeature(dstLayer, f)
        C.OGR_F_Destroy(f)
    }
    if mp != g {
        C.OGR_G_DestroyGeometry(mp)
    }
    }

    for dn, coll := range collByDN {
    // OGR_G_UnionCascaded вызывает GEOSUnaryUnion — быстрый алгоритм
    // слияния произвольного набора геометрий за одну операцию.
    // Результат заменяет coll; оригинальная коллекция уничтожается ниже.
    merged := C.OGR_G_UnionCascaded(coll)
    // Уничтожаем исходную коллекцию — merged уже независим
    C.OGR_G_DestroyGeometry(coll)
    collByDN[dn] = nil // предотвращаем двойной free в defer
    if merged == nil {
        continue
    }
    writeGeom(merged, dn)
    C.OGR_G_DestroyGeometry(merged)
    }

    C.OGR_DS_SyncToDisk(dstDS)
    return nil
}

// ShapefileDifference вычитает полигоны baseShp из newShp.
// Возвращает шейпфайл с полигонами new MINUS base, отфильтрованными по площади.
func ShapefileDifference(newShp, baseShp string, minArea float64) (string, error) {
    initGeom()

    // Открываем оба слоя.
    newCP := C.CString(newShp)
    defer C.free(unsafe.Pointer(newCP))
    newDS := C.GDALOpenEx(newCP, C.GDAL_OF_VECTOR, nil, nil, nil)
    if newDS == nil {
    return "", fmt.Errorf("open new shp: %s", newShp)
    }
    defer C.GDALClose(newDS)
    newLayer := C.GDALDatasetGetLayer(newDS, 0)

    baseCP := C.CString(baseShp)
    defer C.free(unsafe.Pointer(baseCP))
    baseDS := C.GDALOpenEx(baseCP, C.GDAL_OF_VECTOR, nil, nil, nil)
    if baseDS == nil {
    return "", fmt.Errorf("open base shp: %s", baseShp)
    }
    defer C.GDALClose(baseDS)
    baseLayer := C.GDALDatasetGetLayer(baseDS, 0)

    // Собираем все базовые геометрии в срез (без Union — избегаем O(N²)).
    var baseGeoms []C.OGRGeometryH
    C.OGR_L_ResetReading(baseLayer)
    for {
    feat := C.OGR_L_GetNextFeature(baseLayer)
    if feat == nil {
        break
    }
    geom := C.OGR_F_GetGeometryRef(feat)
    if geom != nil {
        baseGeoms = append(baseGeoms, C.OGR_G_Clone(geom))
    }
    C.OGR_F_Destroy(feat)
    }
    defer func() {
    for _, g := range baseGeoms {
        C.OGR_G_DestroyGeometry(g)
    }
    }()

    // Создаём выходной шейпфайл с той же SRS что у входного (UTM тайла).
    // Важно: CreateEmptyShapefile создаёт WGS84, но геометрии в UTM →
    // clip по AOI будет пустым. Берём SRS из newLayer.
    outShp := strings.TrimSuffix(newShp, ".shp") + "_diff.shp"
    {
    base := strings.TrimSuffix(outShp, ".shp")
    for _, ext := range []string{".shp", ".shx", ".dbf", ".prj", ".cpg"} {
        _ = os.Remove(base + ext)
    }
    }
    drvNameC := C.CString("ESRI Shapefile")
    defer C.free(unsafe.Pointer(drvNameC))
    outDrv := C.OGRGetDriverByName(drvNameC)
    outShpC := C.CString(outShp)
    defer C.free(unsafe.Pointer(outShpC))
    outRawDS := C.OGR_Dr_CreateDataSource(outDrv, outShpC, nil)
    if outRawDS == nil {
    return "", fmt.Errorf("create out shp: %s", outShp)
    }
    outLayerNameC := C.CString("result")
    defer C.free(unsafe.Pointer(outLayerNameC))
    // SRS берём из newLayer — это UTM проекция тайла Sentinel.
    newSRS := C.OGR_L_GetSpatialRef(newLayer)
    outRawLayer := C.OGR_DS_CreateLayer(outRawDS, outLayerNameC, newSRS, C.wkbMultiPolygon, nil)
    if outRawLayer == nil {
    C.OGR_DS_Destroy(outRawDS)
    return "", fmt.Errorf("create out layer")
    }
    outDefn := C.OGR_L_GetLayerDefn(outRawLayer)

    // Для каждого нового полигона вычитаем только пересекающиеся базовые.
    // Это быстрее чем Union всего базового: OGR_G_Intersects — дешёвая проверка,
    // OGR_G_Difference вызывается только при реальном пересечении.
    C.OGR_L_ResetReading(newLayer)
    for {
    feat := C.OGR_L_GetNextFeature(newLayer)
    if feat == nil {
        break
    }
    geom := C.OGR_F_GetGeometryRef(feat)
    if geom == nil {
        C.OGR_F_Destroy(feat)
        continue
    }
    result := C.OGR_G_Clone(geom)
    C.OGR_F_Destroy(feat)
    for _, bg := range baseGeoms {
        if result == nil {
	break
        }
        if C.OGR_G_Intersects(result, bg) == 0 {
	continue
        }
        diff := C.OGR_G_Difference(result, bg)
        C.OGR_G_DestroyGeometry(result)
        result = diff
    }
    if result == nil {
        continue
    }
    area := float64(C.OGR_G_Area(result))
    if minArea > 0 && area < minArea {
        C.OGR_G_DestroyGeometry(result)
        continue
    }
    outFeat := C.OGR_F_Create(outDefn)
    if outFeat != nil {
        C.OGR_F_SetGeometryDirectly(outFeat, result)
        C.OGR_L_CreateFeature(outRawLayer, outFeat)
        C.OGR_F_Destroy(outFeat)
    } else {
        C.OGR_G_DestroyGeometry(result)
    }
    }
    C.OGR_DS_SyncToDisk(outRawDS)
    C.OGR_DS_Destroy(outRawDS)
    return outShp, nil
}

// CountShapefileFeatures возвращает число features в shapefile (для диагностики).
func CountShapefileFeatures(shp string) int {
    if shp == "" {
    return 0
    }
    cp := C.CString(shp)
    defer C.free(unsafe.Pointer(cp))
    ds := C.GDALOpenEx(cp, C.GDAL_OF_VECTOR, nil, nil, nil)
    if ds == nil {
    return 0
    }
    defer C.GDALClose(ds)
    lyr := C.GDALDatasetGetLayer(ds, 0)
    if lyr == nil {
    return 0
    }
    return int(C.OGR_L_GetFeatureCount(lyr, 1))
}