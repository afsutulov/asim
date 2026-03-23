package main

import (
	"fmt"
	"os"
	"sync"

	ort "github.com/yalue/onnxruntime_go"
)

var ortOnce sync.Once
var ortInitErr error

func initORT() error {
	ortOnce.Do(func() {
		lib := os.Getenv("ONNXRUNTIME_SHARED_LIBRARY_PATH")
		if lib != "" {
			ort.SetSharedLibraryPath(lib)
		}
		ortInitErr = ort.InitializeEnvironment()
	})
	return ortInitErr
}

type ORTSession struct {
	sess *ort.DynamicAdvancedSession
}

func NewORTSession(modelPath string, device string, cudaDeviceID int) (*ORTSession, error) {
	if err := initORT(); err != nil {
		return nil, fmt.Errorf("onnxruntime init: %w", err)
	}

	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, err
	}
	defer opts.Destroy()

	// CUDA execution provider (requires CUDA-enabled ONNX Runtime build).
	if device == "cuda" {
		cudaOpts, err := ort.NewCUDAProviderOptions()
		if err != nil {
			return nil, fmt.Errorf("cuda execution provider not available in this onnxruntime_go build: %w", err)
		}
		_ = cudaOpts.Update(map[string]string{
			"device_id": fmt.Sprintf("%d", cudaDeviceID),
		})
		if err2 := opts.AppendExecutionProviderCUDA(cudaOpts); err2 != nil {
			_ = cudaOpts.Destroy()
			return nil, fmt.Errorf("failed to enable CUDA execution provider: %w", err2)
		}
		_ = cudaOpts.Destroy()
	}

	s, err := ort.NewDynamicAdvancedSession(modelPath, []string{"input"}, []string{"output"}, opts)
	if err != nil {
		return nil, err
	}
	return &ORTSession{sess: s}, nil
}

func (s *ORTSession) Close() error {
	if s.sess != nil {
		return s.sess.Destroy()
	}
	return nil
}

// Predict runs the model for a batch of tiles.
// input: NCHW float32, shape [N,4,H,W]
// output: N1HW float32, shape [N,1,H,W]
func (s *ORTSession) Predict(input []float32, batch, channels, height, width, outChannels int) ([]float32, error) {
	inShape := ort.NewShape(int64(batch), int64(channels), int64(height), int64(width))
	inTensor, err := ort.NewTensor[float32](inShape, input)
	if err != nil {
		return nil, err
	}
	defer inTensor.Destroy()

	outShape := ort.NewShape(int64(batch), int64(outChannels), int64(height), int64(width))
	outTensor, err := ort.NewEmptyTensor[float32](outShape)
	if err != nil {
		return nil, err
	}
	defer outTensor.Destroy()

	inputs := []ort.Value{inTensor}
	outputs := []ort.Value{outTensor}

	if err := s.sess.Run(inputs, outputs); err != nil {
		return nil, err
	}

	out := outTensor.GetData()
	cpy := make([]float32, len(out))
	copy(cpy, out)
	return cpy, nil
}
