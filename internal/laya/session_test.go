package laya

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/omer-ulucan/baton/internal/laya/model"
)

// libPath finds libonnxruntime: $ONNXRUNTIME_LIB_PATH or the local cache.
func libPath(t testing.TB) string {
	t.Helper()
	if p := os.Getenv("ONNXRUNTIME_LIB_PATH"); p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	p := filepath.Join(home, ".cache", "baton", "onnxruntime", "1.29.0", "lib", "libonnxruntime.so")
	if _, err := os.Stat(p); err != nil {
		t.Skip("libonnxruntime not found; set ONNXRUNTIME_LIB_PATH")
	}
	return p
}

var (
	shared     *Model
	sharedErr  error
	sharedOnce sync.Once
)

// loadModel loads the full fp32 model once for the package's tests.
func loadModel(t testing.TB) *Model {
	t.Helper()
	dir := modelDir(t)
	files, _ := model.Files(model.FP32, false)
	if _, err := os.Stat(filepath.Join(dir, files[len(files)-1].Path)); err != nil {
		t.Skip("fp32 weights not downloaded; run `baton model fetch`")
	}
	lib := libPath(t)
	sharedOnce.Do(func() {
		shared, sharedErr = Load(SessionConfig{LibPath: lib, ModelDir: dir, Variant: model.FP32})
	})
	if sharedErr != nil {
		t.Fatal(sharedErr)
	}
	return shared
}

// TestInferenceParity: Go builds the rows, runs them through ONNX Runtime
// and calibrates; probabilities must match laya's within 1e-4.
func TestInferenceParity(t *testing.T) {
	m := loadModel(t)
	worst := 0.0
	for ci, c := range loadFixtures(t) {
		for qid, fq := range c.Questions {
			seq, err := m.Build(c.State, fq.question(t))
			if err != nil {
				t.Fatal(err)
			}
			logits, err := m.Logits(t.Context(), seq)
			if err != nil {
				t.Fatal(err)
			}
			p := m.Config().Calibrate(seq.QType, logits)
			for i := range p {
				d := math.Abs(p[i] - fq.Probabilities[i])
				worst = max(worst, d)
				if d > 1e-4 {
					t.Errorf("case %d %s: p[%d] = %.6f, laya %.6f (|Δ| %.2e)", ci, qid, i, p[i], fq.Probabilities[i], d)
				}
			}
		}
	}
	t.Logf("max |Δp| vs laya: %.2e", worst)
}

// TestBatchedEqualsSingle: rows of different lengths batched together
// (padded) give the same answers as one at a time.
func TestBatchedEqualsSingle(t *testing.T) {
	m := loadModel(t)
	cases := loadFixtures(t)
	type row struct {
		seq    Sequence
		single []float32
	}
	var rows []row
	for _, c := range cases[:8] {
		for _, fq := range c.Questions {
			seq, _ := m.Build(c.State, fq.question(t))
			one, err := m.Logits(t.Context(), seq)
			if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row{seq, one})
		}
	}
	var wg sync.WaitGroup
	got := make([][]float32, len(rows))
	for i, r := range rows {
		wg.Go(func() {
			l, err := m.Logits(t.Context(), r.seq)
			if err != nil {
				t.Error(err)
			}
			got[i] = l
		})
	}
	wg.Wait()
	for i, r := range rows {
		for j := range r.single {
			if d := math.Abs(float64(got[i][j] - r.single[j])); d > 1e-3 {
				t.Errorf("row %d logit %d: batched %v, single %v", i, j, got[i][j], r.single[j])
			}
		}
	}
}

func TestDeadlineIsHonoured(t *testing.T) {
	m := loadModel(t)
	c := loadFixtures(t)[0]
	var fq fixtureQuestion
	for _, q := range c.Questions {
		fq = q
	}
	seq, _ := m.Build(c.State, fq.question(t))
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := m.Logits(ctx, seq)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 50*time.Millisecond {
		t.Fatalf("err = %v after %s; a caller must not wait for the run", err, time.Since(start))
	}
}

func TestLoadRejectsCorruptModel(t *testing.T) {
	dir := t.TempDir()
	files, _ := model.Files(model.FP32, false)
	for _, f := range files {
		_ = os.MkdirAll(filepath.Join(dir, filepath.Dir(f.Path)), 0o755)
		_ = os.WriteFile(filepath.Join(dir, f.Path), []byte("corrupt"), 0o600)
	}
	if _, err := Load(SessionConfig{LibPath: "/nonexistent/libonnxruntime.so", ModelDir: dir}); !errors.Is(err, model.ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

// BenchmarkLatency measures one decision end to end (build, queue, run,
// calibrate) for the triage question.
func BenchmarkLatency(b *testing.B) {
	m := loadModel(b)
	c := loadFixtures(b)[1]
	seq, err := m.Build(c.State, c.Questions["triage"].question(b))
	if err != nil {
		b.Fatal(err)
	}
	var lat []time.Duration
	for b.Loop() {
		start := time.Now()
		if _, err := m.Logits(b.Context(), seq); err != nil {
			b.Fatal(err)
		}
		lat = append(lat, time.Since(start))
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	b.ReportMetric(float64(lat[len(lat)/2].Milliseconds()), "p50-ms")
	b.ReportMetric(float64(lat[len(lat)*99/100].Milliseconds()), "p99-ms")
}
