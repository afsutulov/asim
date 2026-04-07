package main

// applyMaskFilter очищает маску перед polygonize.
// Для многоклассовых масок компоненты считаются отдельно по каждому значению DN > 0.
func applyMaskFilter(mask []float32, w, h int, spec MaskFilterSpec) []float32 {
	if !spec.Enabled || len(mask) == 0 || w <= 0 || h <= 0 || spec.MinArea <= 1 {
		return mask
	}
	visited := make([]bool, len(mask))
	out := make([]float32, len(mask))
	dirs := [][2]int{{1, 0}, {-1, 0}, {0, 1}, {0, -1}}
	if spec.Connectivity == 8 {
		dirs = append(dirs, [2]int{1, 1}, [2]int{1, -1}, [2]int{-1, 1}, [2]int{-1, -1})
	}
	for i, v := range mask {
		if visited[i] || v == 0 {
			continue
		}
		label := v
		queue := []int{i}
		component := []int{i}
		visited[i] = true
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			x := cur % w
			y := cur / w
			for _, d := range dirs {
				nx, ny := x+d[0], y+d[1]
				if nx < 0 || ny < 0 || nx >= w || ny >= h {
					continue
				}
				ni := ny*w + nx
				if visited[ni] || mask[ni] != label {
					continue
				}
				visited[ni] = true
				queue = append(queue, ni)
				component = append(component, ni)
			}
		}
		if len(component) >= spec.MinArea {
			for _, idx := range component {
				out[idx] = label
			}
		}
	}
	return out
}
