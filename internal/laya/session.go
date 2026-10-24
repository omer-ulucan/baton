package laya

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	ort "github.com/yalue/onnxruntime_go"

	"github.com/omer-ulucan/baton/internal/laya/model"
	"github.com/omer-ulucan/baton/internal/laya/tokenizer"
)

// ErrOverloaded means the inference queue is full; the caller should fall
// back rather than wait (SPEC 10.2).
var ErrOverloaded = errors.New("laya: inference queue full")

// SessionConfig configures a Model.
type SessionConfig struct {
	LibPath  string // libonnxruntime.so
	ModelDir string // pinned model files, verified on load
	Variant  model.Variant
	// IntraOpThreads per run; 0 picks half the CPUs (two runs in flight).
	IntraOpThreads int
	MaxBatch       int           // rows per run, default 8
	MaxWait        time.Duration // batching window, default 5ms
	MaxInflight    int           // concurrent runs, default 2
	// SkipVerify skips hashing the weights at load (tests that just did).
	SkipVerify bool
}

// Model is a loaded Laya session with its tokenizer and calibration. Load
// it once per process; it is safe for concurrent use.
type Model struct {
	tok      *tokenizer.Tokenizer
	specials Specials
	cfg      Config
	session  *ort.DynamicAdvancedSession
	revision string

	queue   chan *request
	sem     chan struct{}
	maxWait time.Duration
	batch   int
	stop    chan struct{}
	wg      sync.WaitGroup
}

type request struct {
	ctx  context.Context
	seq  Sequence
	done chan result
}

type result struct {
	logits []float32
	err    error
}

// ONNX Runtime has one process-wide environment.
var (
	envOnce sync.Once
	envErr  error
	envLib  string
)

func initEnvironment(lib string) error {
	envOnce.Do(func() {
		envLib = lib
		ort.SetSharedLibraryPath(lib)
		envErr = ort.InitializeEnvironment()
	})
	if envErr == nil && lib != envLib {
		return fmt.Errorf("laya: onnxruntime already initialized from %s", envLib)
	}
	return envErr
}

// Load verifies the model files, loads the tokenizer and config, and opens
// the ONNX Runtime session.
func Load(c SessionConfig) (*Model, error) {
	if c.Variant == "" {
		c.Variant = model.FP32
	}
	files, err := model.Files(c.Variant, false)
	if err != nil {
		return nil, err
	}
	if !c.SkipVerify {
		if err := model.Verify(c.ModelDir, files); err != nil {
			return nil, fmt.Errorf("laya: %w", err)
		}
	}
	tok, err := tokenizer.Load(filepath.Join(c.ModelDir, "tokenizer.json"))
	if err != nil {
		return nil, err
	}
	sp, err := LookupSpecials(tok)
	if err != nil {
		return nil, err
	}
	cfg, err := LoadConfig(filepath.Join(c.ModelDir, "config.json"))
	if err != nil {
		return nil, err
	}
	if err := initEnvironment(c.LibPath); err != nil {
		return nil, fmt.Errorf("laya: onnxruntime: %w", err)
	}
	opts, err := ort.NewSessionOptions()
	if err != nil {
		return nil, err
	}
	defer func() { _ = opts.Destroy() }()
	threads := c.IntraOpThreads
	if threads <= 0 {
		threads = max(1, runtime.NumCPU()/2)
	}
	if err := opts.SetIntraOpNumThreads(threads); err != nil {
		return nil, err
	}
	if err := opts.SetGraphOptimizationLevel(ort.GraphOptimizationLevelEnableAll); err != nil {
		return nil, err
	}
	sess, err := ort.NewDynamicAdvancedSession(filepath.Join(c.ModelDir, model.GraphPath(c.Variant)),
		[]string{"input_ids", "attention_mask", "marker_pos", "marker_mask", "qtype"},
		[]string{"logits", "act_logits"}, opts)
	if err != nil {
		return nil, fmt.Errorf("laya: open session: %w", err)
	}
	m := &Model{
		tok: tok, specials: sp, cfg: cfg, session: sess, revision: model.RevisionLabel(c.Variant),
		batch: orInt(c.MaxBatch, 8), maxWait: c.MaxWait, stop: make(chan struct{}),
		sem: make(chan struct{}, orInt(c.MaxInflight, 2)),
	}
	if m.maxWait <= 0 {
		m.maxWait = 5 * time.Millisecond
	}
	m.queue = make(chan *request, m.batch*cap(m.sem)*2)
	m.wg.Go(m.batcher)
	return m, nil
}

func orInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// Close stops the batcher and releases the session.
func (m *Model) Close() error {
	close(m.stop)
	m.wg.Wait()
	return m.session.Destroy()
}

// Revision is recorded with every decision this model makes.
func (m *Model) Revision() string { return m.revision }

// Config is the checkpoint's laya configuration.
func (m *Model) Config() Config { return m.cfg }

// Build tokenizes a state and question into a model row.
func (m *Model) Build(state string, q Question) (Sequence, error) {
	return BuildSequence(m.tok, m.specials, m.cfg, EncodeState(m.tok, state), q)
}

// Logits runs one row through the model, batched with concurrent calls.
// It returns when the result is ready or ctx ends, whichever is first;
// an ONNX run cannot be interrupted, so a run whose callers gave up
// finishes in the background and its result is dropped.
func (m *Model) Logits(ctx context.Context, seq Sequence) ([]float32, error) {
	r := &request{ctx: ctx, seq: seq, done: make(chan result, 1)}
	select {
	case m.queue <- r:
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, ErrOverloaded
	}
	select {
	case res := <-r.done:
		return res.logits, res.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *Model) batcher() {
	var runs sync.WaitGroup
	defer runs.Wait()
	for {
		var first *request
		select {
		case first = <-m.queue:
		case <-m.stop:
			return
		}
		batch := []*request{first}
		timer := time.NewTimer(m.maxWait)
	collect:
		for len(batch) < m.batch {
			select {
			case r := <-m.queue:
				batch = append(batch, r)
			case <-timer.C:
				break collect
			}
		}
		timer.Stop()
		live := batch[:0]
		for _, r := range batch {
			if r.ctx.Err() == nil { // nobody waits for the others
				live = append(live, r)
			}
		}
		if len(live) == 0 {
			continue
		}
		m.sem <- struct{}{}
		runs.Go(func() {
			defer func() { <-m.sem }()
			logits, err := m.run(live)
			for i, r := range live {
				res := result{err: err}
				if err == nil {
					res.logits = logits[i]
				}
				r.done <- res
			}
		})
	}
}

// run collates rows as laya.common.collate_items does and runs the model.
func (m *Model) run(reqs []*request) ([][]float32, error) {
	n := len(reqs)
	seqLen, kmax := 0, 0
	for _, r := range reqs {
		seqLen = max(seqLen, len(r.seq.IDs))
		kmax = max(kmax, len(r.seq.Markers))
	}
	ids := make([]int64, n*seqLen)
	att := make([]int64, n*seqLen)
	mpos := make([]int64, n*kmax)
	mmask := make([]bool, n*kmax)
	qtype := make([]int64, n)
	for i, r := range reqs {
		for j := range seqLen {
			ids[i*seqLen+j] = int64(m.specials.Pad)
		}
		for j, id := range r.seq.IDs {
			ids[i*seqLen+j] = int64(id)
			att[i*seqLen+j] = 1
		}
		for j, p := range r.seq.Markers {
			mpos[i*kmax+j] = int64(p)
			mmask[i*kmax+j] = true
		}
		qtype[i] = int64(r.seq.QType)
	}
	var inputs []ort.Value
	defer func() {
		for _, v := range inputs {
			_ = v.Destroy()
		}
	}()
	add := func(v ort.Value, err error) error {
		if err == nil {
			inputs = append(inputs, v)
		}
		return err
	}
	if err := errors.Join(
		add(ort.NewTensor(ort.NewShape(int64(n), int64(seqLen)), ids)),
		add(ort.NewTensor(ort.NewShape(int64(n), int64(seqLen)), att)),
		add(ort.NewTensor(ort.NewShape(int64(n), int64(kmax)), mpos)),
		add(ort.NewTensor(ort.NewShape(int64(n), int64(kmax)), mmask)),
		add(ort.NewTensor(ort.NewShape(int64(n)), qtype)),
	); err != nil {
		return nil, fmt.Errorf("laya: build tensors: %w", err)
	}
	outputs := []ort.Value{nil, nil} // allocated by onnxruntime (dynamic shapes)
	if err := m.session.Run(inputs, outputs); err != nil {
		return nil, fmt.Errorf("laya: run: %w", err)
	}
	defer func() {
		for _, v := range outputs {
			if v != nil {
				_ = v.Destroy()
			}
		}
	}()
	lt, ok := outputs[0].(*ort.Tensor[float32])
	if !ok {
		return nil, fmt.Errorf("laya: logits output is %T, want float32 tensor", outputs[0])
	}
	data := lt.GetData()
	out := make([][]float32, n)
	for i, r := range reqs {
		k := len(r.seq.Markers)
		out[i] = append([]float32(nil), data[i*kmax:i*kmax+k]...)
	}
	return out, nil
}
