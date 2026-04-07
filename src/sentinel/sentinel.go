package main

import (
	"encoding/xml"
	"fmt"
	"io"
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

func normalizePreprocess(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return "sentinel"
	}
	return mode
}

func normalizeResolution(res string) string {
	switch strings.ToUpper(strings.TrimSpace(res)) {
	case "R20M", "R20m":
		return "R20m"
	case "R60M", "R60m":
		return "R60m"
	default:
		return "R10m"
	}
}

func l2aResolutionOrder(spec ModelSpec) []string {
	switch normalizeResolution(spec.Resolution) {
	case "R20m":
		return []string{"20m", "10m", "60m"}
	case "R60m":
		return []string{"60m", "20m", "10m"}
	default:
		return []string{"10m", "20m", "60m"}
	}
}

func recursiveJP2Matches(root, suffix string) []string {
	matches := make([]string, 0)
	if strings.TrimSpace(root) == "" {
		return matches
	}
	_ = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d == nil || d.IsDir() {
			return nil
		}
		name := strings.ToUpper(d.Name())
		if strings.HasSuffix(name, strings.ToUpper(suffix)) {
			matches = append(matches, path)
		}
		return nil
	})
	sort.Strings(matches)
	return matches
}

func safeRootFromImgDataPath(imgDataPath string) string {
	cur := filepath.Clean(strings.TrimSpace(imgDataPath))
	if cur == "." || cur == "" {
		return ""
	}
	for {
		base := filepath.Base(cur)
		if strings.HasSuffix(strings.ToUpper(base), ".SAFE") {
			return cur
		}
		next := filepath.Dir(cur)
		if next == cur {
			break
		}
		cur = next
	}
	return ""
}

func l2aMetadataBandMatches(safeRoot, band string, spec ModelSpec) []string {
	band = strings.ToUpper(strings.TrimSpace(band))
	if safeRoot == "" || band == "" {
		return nil
	}
	metaPath := filepath.Join(safeRoot, "MTD_MSIL2A.xml")
	f, err := os.Open(metaPath)
	if err != nil {
		return nil
	}
	defer f.Close()

	resOrder := l2aResolutionOrder(spec)
	resRank := map[string]int{"10M": 100, "20M": 200, "60M": 300}
	for idx, res := range resOrder {
		resRank[strings.ToUpper(strings.TrimPrefix(res, "R"))] = idx
	}

	type cand struct {
		path string
		rank int
	}
	var found []cand
	dec := xml.NewDecoder(f)
	for {
		tok, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil
		}
		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "IMAGE_FILE" {
			continue
		}
		var rel string
		if err := dec.DecodeElement(&rel, &se); err != nil {
			continue
		}
		rel = strings.TrimSpace(rel)
		if rel == "" {
			continue
		}
		upperRel := strings.ToUpper(rel)
		if !strings.Contains(upperRel, "_"+band+"_") && !strings.HasSuffix(upperRel, "_"+band) {
			continue
		}
		full := filepath.Join(safeRoot, filepath.FromSlash(rel)+".jp2")
		if _, err := os.Stat(full); err != nil {
			full = filepath.Join(safeRoot, filepath.FromSlash(rel)+".JP2")
			if _, err := os.Stat(full); err != nil {
				continue
			}
		}
		rank := 1000
		for key, val := range resRank {
			if strings.Contains(upperRel, "_"+key) || strings.Contains(upperRel, "/R"+key) {
				rank = val
				break
			}
		}
		found = append(found, cand{path: full, rank: rank})
	}
	if len(found) == 0 {
		return nil
	}
	sort.Slice(found, func(i, j int) bool {
		if found[i].rank != found[j].rank {
			return found[i].rank < found[j].rank
		}
		return found[i].path < found[j].path
	})
	out := make([]string, 0, len(found))
	for _, c := range found {
		out = append(out, c.path)
	}
	return out
}

func bandFileForSpec(tile SafeTile, band string, spec ModelSpec) string {
	band = strings.ToUpper(strings.TrimSpace(band))
	mode := normalizePreprocess(spec.Preprocess)
	if mode != "sentinel2a" {
		return bandFile(tile, band)
	}
	root := strings.TrimSpace(tile.ImgDataPath)
	if root == "" {
		return filepath.Join(root, "MISSING_"+band+".jp2")
	}
	safeRoot := safeRootFromImgDataPath(root)
	if matches := l2aMetadataBandMatches(safeRoot, band, spec); len(matches) > 0 {
		return matches[0]
	}
	order := l2aResolutionOrder(spec)
	for _, res := range order {
		sfx := "_" + band + "_" + strings.ToUpper(strings.TrimPrefix(res, "R")) + ".JP2"
		if matches := recursiveJP2Matches(root, sfx); len(matches) > 0 {
			return matches[0]
		}
		dir := filepath.Join(root, res)
		if matches := recursiveJP2Matches(dir, sfx); len(matches) > 0 {
			return matches[0]
		}
	}
	if matches := recursiveJP2Matches(root, "_"+band+".JP2"); len(matches) > 0 {
		return matches[0]
	}
	for _, res := range []string{"10M", "20M", "60M"} {
		if matches := recursiveJP2Matches(root, "_"+band+"_"+res+".JP2"); len(matches) > 0 {
			return matches[0]
		}
	}
	return filepath.Join(root, "MISSING_"+band+".jp2")
}

func referenceBandCandidates(spec ModelSpec) []string {
	if normalizePreprocess(spec.Preprocess) != "sentinel2a" {
		return []string{"B02", "B03", "B04", "B08", "B11", "B12", "B10"}
	}
	switch normalizeResolution(spec.Resolution) {
	case "R20m":
		return []string{"B05", "B06", "B07", "B8A", "B11", "B12", "SCL", "B02", "B03", "B04"}
	case "R60m":
		return []string{"B01", "B09", "B11", "B12", "SCL", "B02", "B03", "B04"}
	default:
		return []string{"B02", "B03", "B04", "B08", "B11", "B12", "SCL"}
	}
}

func bandFile(tile SafeTile, band string) string {
    suffix := "_" + strings.ToUpper(strings.TrimSpace(band)) + ".JP2"
    if matches := recursiveJP2Matches(tile.ImgDataPath, suffix); len(matches) > 0 {
	return matches[0]
    }
    return filepath.Join(tile.ImgDataPath, "MISSING_"+band+".jp2")
}

func firstJP2InDir(imgDataPath string) (string, error) {
	entries, err := os.ReadDir(imgDataPath)
	if err == nil {
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
	}
	matches := recursiveJP2Matches(imgDataPath, ".JP2")
	for _, m := range matches {
		lower := strings.ToLower(filepath.Base(m))
		if strings.Contains(lower, "_tci_") || strings.Contains(lower, "_tci.") || strings.Contains(lower, "_aot_") || strings.Contains(lower, "_wvp_") {
			continue
		}
		return m, nil
	}
	return "", fmt.Errorf("no jp2 found in %s", imgDataPath)
}
