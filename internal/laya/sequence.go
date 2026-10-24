// Package laya runs the Laya decision model in-process: the input
// builder and calibration ported from laya 0.3.21 (laya.common), and an
// ONNX Runtime session that batches concurrent decisions under a bounded
// concurrency (ADR 0007, ADR 0009).
package laya

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
)

// QType is a question type; the values are the model's qtype input.
type QType int

const (
	Choice QType = 0
	Score  QType = 1
	Noul   QType = 2
)

func (q QType) String() string {
	switch q {
	case Choice:
		return "choice"
	case Score:
		return "score"
	case Noul:
		return "noul"
	}
	return fmt.Sprintf("qtype(%d)", int(q))
}

// Option is a choice label with an optional description.
type Option struct {
	Label       string
	Description string
}

// Question is one typed question, in laya's internal form.
type Question struct {
	Type         QType
	Instructions string
	// Options: the labels of a choice question; the levels of a score
	// question (Label holds the level text); for noul, optional
	// descriptions of the false and true answers, in that order.
	Options []Option
}

// Config is the "laya" section of the export's config.json.
type Config struct {
	MaxLen               int                `json:"max_len"`
	HeadMaxLen           int                `json:"head_max_len"`
	MaxOptionTokens      int                `json:"max_option_tokens"`
	Temperature          []float64          `json:"temperature"`
	TemperatureByOptions map[string]float64 `json:"temperature_by_options"`
	TemperatureClamp     []float64          `json:"temperature_clamp"`
}

// LoadConfig reads config.json.
func LoadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var f struct {
		Laya *Config `json:"laya"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return Config{}, fmt.Errorf("config.json: %w", err)
	}
	if f.Laya == nil {
		return Config{}, fmt.Errorf("config.json: no laya section")
	}
	c := *f.Laya
	switch {
	case c.MaxLen <= 0 || c.HeadMaxLen <= 0 || c.HeadMaxLen >= c.MaxLen:
		return Config{}, fmt.Errorf("config.json: bad max_len %d / head_max_len %d", c.MaxLen, c.HeadMaxLen)
	case len(c.Temperature) != 3:
		return Config{}, fmt.Errorf("config.json: temperature must have 3 entries, got %d", len(c.Temperature))
	}
	if c.MaxOptionTokens <= 0 {
		c.MaxOptionTokens = 48 // hard-coded in laya.common.build_sequence
	}
	if len(c.TemperatureClamp) != 2 {
		c.TemperatureClamp = []float64{0.5, 5.0} // laya.common TEMP_MIN, TEMP_MAX
	}
	return c, nil
}

// Encoder is the tokenizer as the builder needs it.
type Encoder interface {
	Encode(text string) []int
	TokenID(content string) (int, bool)
}

// Special token ids the builder places.
type Specials struct {
	CLS, SEP, Mask, Pad int
}

// LookupSpecials finds the special tokens laya uses.
func LookupSpecials(enc Encoder) (Specials, error) {
	var s Specials
	for name, dst := range map[string]*int{"[CLS]": &s.CLS, "[SEP]": &s.SEP, "[MASK]": &s.Mask, "[PAD]": &s.Pad} {
		id, ok := enc.TokenID(name)
		if !ok {
			return Specials{}, fmt.Errorf("tokenizer has no %s token", name)
		}
		*dst = id
	}
	return s, nil
}

const maskText = "[MASK]"

// RenderOptions renders option texts in label order, as
// laya.common.render_options does for string criteria.
func RenderOptions(q Question) []string {
	switch q.Type {
	case Choice:
		out := make([]string, len(q.Options))
		for i, o := range q.Options {
			out[i] = o.Label
			if o.Description != "" {
				out[i] = o.Label + ": " + o.Description
			}
		}
		return out
	case Score:
		out := make([]string, len(q.Options))
		for i, o := range q.Options {
			out[i] = fmt.Sprintf("level %d: %s", i, o.Label)
		}
		return out
	default:
		falseDesc, trueDesc := "no, the statement does not hold", "yes, the statement holds"
		if len(q.Options) > 0 && q.Options[0].Description != "" {
			falseDesc = q.Options[0].Description
		}
		if len(q.Options) > 1 && q.Options[1].Description != "" {
			trueDesc = q.Options[1].Description
		}
		return []string{"false: " + falseDesc, "true: " + trueDesc}
	}
}

// Sequence is one model input row.
type Sequence struct {
	IDs     []int
	Markers []int // positions of the option [MASK] markers
	QType   QType
}

// BuildSequence is laya.common.build_sequence (laya 0.3.21) for a string
// state and default option order:
//
//	[CLS] <type> question: <instructions> [SEP] [MASK] opt0 [MASK] opt1 ... [SEP] state [SEP]
//
// stateIDs is the tokenized state, shared by all questions about it.
func BuildSequence(enc Encoder, sp Specials, cfg Config, stateIDs []int, q Question) (Sequence, error) {
	opts := RenderOptions(q)
	head := enc.Encode(q.Type.String() + " question: " + strings.ReplaceAll(q.Instructions, maskText, " "))
	optIDs := make([][]int, len(opts))
	for i, o := range opts {
		toks := enc.Encode(" " + strings.ReplaceAll(o, maskText, " "))
		if len(toks) > cfg.MaxOptionTokens {
			toks = toks[:cfg.MaxOptionTokens]
		}
		optIDs[i] = append([]int{sp.Mask}, toks...)
	}
	budget := cfg.HeadMaxLen - totalLen(optIDs)
	if budget < 16 {
		per := max(4, (cfg.HeadMaxLen-16)/max(1, len(optIDs)))
		for i := range optIDs {
			if len(optIDs[i]) > per {
				optIDs[i] = optIDs[i][:per]
			}
		}
		budget = cfg.HeadMaxLen - totalLen(optIDs)
	}
	if n := max(8, budget); len(head) > n {
		head = head[:n]
	}
	ids := make([]int, 0, cfg.MaxLen)
	ids = append(ids, sp.CLS)
	ids = append(ids, head...)
	ids = append(ids, sp.SEP)
	markers := make([]int, 0, len(optIDs))
	for _, o := range optIDs {
		markers = append(markers, len(ids))
		ids = append(ids, o...)
	}
	ids = append(ids, sp.SEP)
	room := max(0, cfg.MaxLen-len(ids)-1)
	st := stateIDs
	if len(st) > room {
		st = st[:room]
	}
	ids = append(ids, st...)
	ids = append(ids, sp.SEP)
	if len(ids) > cfg.MaxLen {
		ids = ids[:cfg.MaxLen]
	}
	kept := markers[:0]
	for _, m := range markers {
		if m < cfg.MaxLen {
			kept = append(kept, m)
		}
	}
	if len(kept) != len(opts) {
		// laya's Agent refuses these too: some options fell past max_len.
		return Sequence{}, fmt.Errorf("question needs %d tokens for %d options, more than max_len %d allows", len(ids), len(opts), cfg.MaxLen)
	}
	return Sequence{IDs: ids, Markers: kept, QType: q.Type}, nil
}

// EncodeState tokenizes a state for BuildSequence.
func EncodeState(enc Encoder, state string) []int {
	return enc.Encode(strings.ReplaceAll(state, maskText, " "))
}

func totalLen(xs [][]int) int {
	n := 0
	for _, x := range xs {
		n += len(x)
	}
	return n
}

// TemperatureFor picks the calibration temperature for a question with k
// options: the per-bucket value if the checkpoint has one, otherwise the
// per-type value, clamped (laya.common temp_bucket, clamp_temperature).
func (c Config) TemperatureFor(q QType, k int) float64 {
	bucket := "11+"
	switch {
	case k <= 2:
		bucket = "2"
	case k <= 5:
		bucket = "3-5"
	case k <= 10:
		bucket = "6-10"
	}
	t, ok := c.TemperatureByOptions[q.String()+":"+bucket]
	if !ok {
		t = c.Temperature[q]
	}
	if math.IsNaN(t) || math.IsInf(t, 0) {
		return 1
	}
	return min(c.TemperatureClamp[1], max(c.TemperatureClamp[0], t))
}

// Calibrate turns a question's marker logits into probabilities:
// softmax(logits / T).
func (c Config) Calibrate(q QType, logits []float32) []float64 {
	t := c.TemperatureFor(q, len(logits))
	p := make([]float64, len(logits))
	zmax := math.Inf(-1)
	for i, l := range logits {
		p[i] = float64(l) / t
		zmax = max(zmax, p[i])
	}
	sum := 0.0
	for i := range p {
		p[i] = math.Exp(p[i] - zmax)
		sum += p[i]
	}
	for i := range p {
		p[i] /= sum
	}
	return p
}
