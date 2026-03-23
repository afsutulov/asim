package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type TileCache struct {
	Version    int             `json:"version"`
	Preprocess string          `json:"preprocess"`
	Generated  string          `json:"generated"`
	Year       int             `json:"year"`
	Tiles      []SafeTileEntry `json:"tiles"`
}

type SafeTileEntry struct {
	Date        string     `json:"date"`
	CapturedAt  string     `json:"captured_at,omitempty"`
	SceneID     string     `json:"scene_id,omitempty"`
	SafeName    string     `json:"safe_name,omitempty"`
	ImgDataPath string     `json:"img_data_path"`
	Cloud       float64    `json:"cloud"`
	Envelope    [4]float64 `json:"envelope"`
	TileID      string     `json:"tile_id"`
}

func DefaultCashPath(configPath string) string {
	p := strings.TrimSpace(configPath)
	if p == "" {
		p = DefaultConfigPath()
	}
	return filepath.Join(filepath.Dir(p), "cash.json")
}

func DefaultCashPathForYear(root string, year int) string {
	return filepath.Join(root, fmt.Sprintf("%04d", year), "cash.json")
}

func LoadTileCache(path string) (*TileCache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cache TileCache
	if err := json.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cache.Preprocess) == "" {
		cache.Preprocess = "sentinel"
	}
	if cache.Version < 7 {
		return nil, fmt.Errorf("outdated cash.json version %d: regenerate cache with current cron", cache.Version)
	}
	if cache.Preprocess != "sentinel" {
		return nil, fmt.Errorf("unsupported cash.json preprocess: %s", cache.Preprocess)
	}
	if len(cache.Tiles) == 0 {
		return nil, fmt.Errorf("cash.json is empty")
	}
	return &cache, nil
}

func collectYears(start, end string) ([]int, error) {
	startDate, err := parseDateOnly(start)
	if err != nil {
		return nil, err
	}
	endDate, err := parseDateOnly(end)
	if err != nil {
		return nil, err
	}
	if endDate.Before(startDate) {
		return nil, fmt.Errorf("end date before start date: %s < %s", end, start)
	}
	years := make([]int, 0)
	for y := startDate.Year(); y <= endDate.Year(); y++ {
		years = append(years, y)
	}
	return years, nil
}

func dedupeTiles(tiles []SafeTile) ([]SafeTile, int) {
	if len(tiles) == 0 {
		return tiles, 0
	}
	seen := make(map[string]SafeTile, len(tiles))
	for _, tile := range tiles {
		key := sceneKey(tile)
		cur, ok := seen[key]
		if !ok {
			seen[key] = tile
			continue
		}
		// Prefer entry with lower cloud; if equal prefer lexicographically last path/safename.
		if tile.Cloud < cur.Cloud ||
			(tile.Cloud == cur.Cloud && strings.TrimSpace(tile.SafeName) > strings.TrimSpace(cur.SafeName)) ||
			(tile.Cloud == cur.Cloud && strings.TrimSpace(tile.SafeName) == strings.TrimSpace(cur.SafeName) && strings.TrimSpace(tile.ImgDataPath) > strings.TrimSpace(cur.ImgDataPath)) {
			seen[key] = tile
		}
	}
	out := make([]SafeTile, 0, len(seen))
	for _, tile := range seen {
		out = append(out, tile)
	}
	sortTiles(out)
	return out, len(tiles) - len(out)
}

func sortTiles(tiles []SafeTile) {
	sort.Slice(tiles, func(i, j int) bool {
		ti := tileTime(tiles[i])
		tj := tileTime(tiles[j])
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		if tiles[i].TileID != tiles[j].TileID {
			return tiles[i].TileID < tiles[j].TileID
		}
		return sceneKey(tiles[i]) < sceneKey(tiles[j])
	})
}

func tileTime(tile SafeTile) time.Time {
	if t, err := parseCapturedAt(tile.CapturedAt); err == nil {
		return t
	}
	t, _ := parseDateOnly(tile.Date)
	return t
}

func entryToTile(item SafeTileEntry) SafeTile {
	captured := strings.TrimSpace(item.CapturedAt)
	if captured == "" {
		captured = strings.TrimSpace(item.Date)
	}
	sceneID := strings.TrimSpace(item.SceneID)
	if sceneID == "" {
		sceneID = strings.TrimSpace(item.SafeName)
	}
	return SafeTile{
		Date:        item.Date,
		CapturedAt:  captured,
		SceneID:     sceneID,
		SafeName:    item.SafeName,
		ImgDataPath: item.ImgDataPath,
		Cloud:       item.Cloud,
		Envelope:    item.Envelope,
		TileID:      item.TileID,
	}
}

func LoadTilesForPeriod(root, start, end string, cloudLimit float64) ([]SafeTile, error) {
	years, err := collectYears(start, end)
	if err != nil {
		return nil, err
	}
	startDate, _ := parseDateOnly(start)
	endDate, _ := parseDateOnly(end)
	out := make([]SafeTile, 0)
	for _, year := range years {
		cachePath := DefaultCashPathForYear(root, year)
		cache, err := LoadTileCache(cachePath)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", cachePath, err)
		}
		for _, item := range cache.Tiles {
			ts, err := parseDateOnly(item.Date)
			if err != nil {
				return nil, fmt.Errorf("bad tile date in %s for %s: %w", cachePath, item.ImgDataPath, err)
			}
			if ts.Before(startDate) || ts.After(endDate) {
				continue
			}
			if item.Cloud > cloudLimit {
				continue
			}
			out = append(out, entryToTile(item))
		}
	}
	out, _ = dedupeTiles(out)
	return out, nil
}

func inferYearFromCashPath(path string) int {
	base := filepath.Base(filepath.Dir(path))
	y, _ := strconv.Atoi(base)
	return y
}

func LoadTilesFromSingleCash(path, start, end string, cloudLimit float64) ([]SafeTile, error) {
	cache, err := LoadTileCache(path)
	if err != nil {
		return nil, err
	}
	startDate, err := parseDateOnly(start)
	if err != nil {
		return nil, err
	}
	endDate, err := parseDateOnly(end)
	if err != nil {
		return nil, err
	}
	out := make([]SafeTile, 0)
	for _, item := range cache.Tiles {
		ts, err := parseDateOnly(item.Date)
		if err != nil {
			return nil, fmt.Errorf("bad tile date in cash.json for %s: %w", item.ImgDataPath, err)
		}
		if ts.Before(startDate) || ts.After(endDate) || item.Cloud > cloudLimit {
			continue
		}
		out = append(out, entryToTile(item))
	}
	out, _ = dedupeTiles(out)
	return out, nil
}
