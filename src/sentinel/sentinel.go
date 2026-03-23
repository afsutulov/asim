package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

type SafeTile struct {
	Date        string
	CapturedAt  string
	SceneID     string
	SafeName    string
	ImgDataPath string
	Cloud       float64
	Envelope    [4]float64
	TileID      string
}

var tileIDRe = regexp.MustCompile(`T\d{2}[A-Z]{3}`)

func parseDateOnly(s string) (time.Time, error) {
	return time.Parse("2006-01-02", strings.TrimSpace(s))
}

func tileIDFromImgDataPath(imgDataPath string) string {
	if m := tileIDRe.FindString(imgDataPath); m != "" {
		return m
	}
	base := filepath.Base(imgDataPath)
	if m := tileIDRe.FindString(base); m != "" {
		return m
	}
	return ""
}

func parseCapturedAt(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, fmt.Errorf("empty captured_at")
	}
	layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "20060102T150405"}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	if len(s) >= 10 {
		return time.Parse("2006-01-02", s[:10])
	}
	return time.Time{}, fmt.Errorf("unsupported captured_at: %s", s)
}

func sceneKey(tile SafeTile) string {
	if strings.TrimSpace(tile.SceneID) != "" {
		return strings.TrimSpace(tile.SceneID)
	}
	if strings.TrimSpace(tile.SafeName) != "" {
		return strings.TrimSpace(tile.SafeName)
	}
	if strings.TrimSpace(tile.CapturedAt) != "" {
		return strings.TrimSpace(tile.TileID) + "|" + strings.TrimSpace(tile.CapturedAt)
	}
	return strings.TrimSpace(tile.TileID) + "|" + strings.TrimSpace(tile.Date) + "|" + strings.TrimSpace(tile.ImgDataPath)
}

func tileLabel(tile SafeTile) string {
	id := strings.TrimSpace(tile.TileID)
	if id == "" {
		id = tileIDFromImgDataPath(tile.ImgDataPath)
	}
	if id == "" {
		id = filepath.Base(filepath.Clean(tile.ImgDataPath))
	}
	return fmt.Sprintf("%s %s", id, tile.Date)
}

func bandFile(tile SafeTile, band string) string {
	pattern := filepath.Join(tile.ImgDataPath, "*_"+strings.ToUpper(strings.TrimSpace(band))+".jp2")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		pattern = filepath.Join(tile.ImgDataPath, "*_"+strings.ToUpper(strings.TrimSpace(band))+".JP2")
		matches, _ = filepath.Glob(pattern)
	}
	if len(matches) == 0 {
		return filepath.Join(tile.ImgDataPath, "MISSING_"+band+".jp2")
	}
	sort.Strings(matches)
	return matches[0]
}

func firstJP2InDir(imgDataPath string) (string, error) {
	entries, err := os.ReadDir(imgDataPath)
	if err != nil {
		return "", err
	}
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		lower := strings.ToLower(name)
		if strings.HasSuffix(lower, ".jp2") && !strings.Contains(lower, "_tci.") {
			return filepath.Join(imgDataPath, name), nil
		}
	}
	return "", fmt.Errorf("no jp2 found in %s", imgDataPath)
}
