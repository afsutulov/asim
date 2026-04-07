package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type AppConfig struct {
	ModelsPath   string
	Logs         string
	Satellite    string
	Sentinel     string
	Sentinel2A   string
	Tmp          string
	Users        string
	PoliPath     string
	PoligonsFile string
	ResultsPath  string
	ResultsFile  string
	Prosessing   string
	Models       map[string]ModelSpec
	Poligons     map[string]PolygonSpec
}

type PolygonSpec struct {
	Name   string `json:"name"`
	Public int    `json:"public"`
}

type rawPaths struct {
	Logs         string `json:"logs"`
	Satellite    string `json:"satellite"`
	Sentinel     string `json:"sentinel"`
	Sentinel2A   string `json:"sentinel2A"`
	Tmp          string `json:"tmp"`
	Users        string `json:"users"`
	PoliPath     string `json:"poli_path"`
	PoligonsFile string `json:"poligons"`
	ResultsPath  string `json:"results_path"`
	ResultsFile  string `json:"results"`
	Prosessing   string `json:"prosessing"`
	ModelsPath   string `json:"models_path"`
}

func DefaultConfigPath() string {
	return filepath.Join("data", "config.json")
}

func LoadAppConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rp rawPaths
	if err := json.Unmarshal(data, &rp); err != nil {
		return nil, err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return nil, err
	}
	var models map[string]ModelSpec
	if blob, ok := top["models"]; ok {
		if err := json.Unmarshal(blob, &models); err != nil {
			return nil, fmt.Errorf("parse models: %w", err)
		}
	}
	cfg := &AppConfig{
		ModelsPath:   rp.ModelsPath,
		Logs:         rp.Logs,
		Satellite:    rp.Satellite,
		Sentinel:     rp.Sentinel,
		Sentinel2A:   rp.Sentinel2A,
		Tmp:          rp.Tmp,
		Users:        rp.Users,
		PoliPath:     rp.PoliPath,
		PoligonsFile: rp.PoligonsFile,
		ResultsPath:  rp.ResultsPath,
		ResultsFile:  rp.ResultsFile,
		Prosessing:   rp.Prosessing,
		Models:       make(map[string]ModelSpec),
		Poligons:     make(map[string]PolygonSpec),
	}
	if cfg.Sentinel == "" {
		cfg.Sentinel = cfg.Satellite
	}
	if strings.TrimSpace(cfg.ModelsPath) == "" {
		return nil, fmt.Errorf("models_path is empty")
	}
	if cfg.ResultsPath == "" {
		return nil, fmt.Errorf("results_path is empty")
	}
	if strings.TrimSpace(cfg.PoligonsFile) == "" {
		return nil, fmt.Errorf("poligons is empty")
	}
	polyData, err := os.ReadFile(cfg.PoligonsFile)
	if err != nil {
		return nil, fmt.Errorf("read poligons json: %w", err)
	}
	if err := json.Unmarshal(polyData, &cfg.Poligons); err != nil {
		return nil, fmt.Errorf("parse poligons json: %w", err)
	}
	filtered := make(map[string]ModelSpec)
	for key, spec := range models {
		if spec.Name == "" {
			spec.Name = key
		}
		spec = FinalizeSpec(spec)
		mode := strings.ToLower(strings.TrimSpace(spec.Preprocess))
		if mode != "sentinel" && mode != "sentinel2a" && mode != "identity" && mode != "pathology" && mode != "pathology_diff" {
			continue
		}
		spec.Preprocess = mode
		if err := ValidateSpec(spec); err != nil {
			return nil, fmt.Errorf("config validation: %w", err)
		}
		filtered[key] = spec
	}
	cfg.Models = filtered
	return cfg, nil
}

func (cfg *AppConfig) PolygonPath(name string) (string, error) {
	if _, ok := cfg.Poligons[name]; !ok {
		return "", fmt.Errorf("polygon %q not found in poligons.json", name)
	}
	if strings.TrimSpace(cfg.PoliPath) == "" {
		return "", fmt.Errorf("poli_path is empty")
	}
	return filepath.Join(cfg.PoliPath, name+".geojson"), nil
}

func (cfg *AppConfig) ModelPath(spec ModelSpec) string {
	if filepath.IsAbs(spec.ONNXFile) || strings.TrimSpace(cfg.ModelsPath) == "" {
		return spec.ONNXFile
	}
	return filepath.Join(cfg.ModelsPath, spec.ONNXFile)
}
