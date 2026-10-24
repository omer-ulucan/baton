package laya

import (
	"bytes"
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"testing"

	"github.com/omer-ulucan/baton/internal/laya/model"
	"github.com/omer-ulucan/baton/internal/laya/tokenizer"
)

// modelDir is the pinned model cache: $BATON_MODEL_DIR or the default.
func modelDir(t testing.TB) string {
	t.Helper()
	dir := os.Getenv("BATON_MODEL_DIR")
	if dir == "" {
		var err error
		if dir, err = model.DefaultDir(); err != nil {
			t.Skip(err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "tokenizer.json")); err != nil {
		t.Skipf("model files not in %s; run `baton model fetch --small` (or full fetch for inference tests)", dir)
	}
	return dir
}

type fixtureQuestion struct {
	Spec struct {
		Type         string          `json:"type"`
		Instructions string          `json:"instructions"`
		Criteria     json.RawMessage `json:"criteria"`
	} `json:"spec"`
	IDs           []int     `json:"ids"`
	Markers       []int     `json:"markers"`
	Logits        []float32 `json:"logits"`
	Temperature   float64   `json:"temperature"`
	Probabilities []float64 `json:"probabilities"`
}

type fixtureCase struct {
	State     string                     `json:"state"`
	Questions map[string]fixtureQuestion `json:"questions"`
}

func loadFixtures(t testing.TB) []fixtureCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Cases []fixtureCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f.Cases
}

// question converts a fixture's laya-API question spec to a Question,
// keeping the criteria order of the JSON object.
func (fq fixtureQuestion) question(t testing.TB) Question {
	t.Helper()
	q := Question{Instructions: fq.Spec.Instructions}
	switch fq.Spec.Type {
	case "choice":
		q.Type = Choice
		q.Options = orderedObject(t, fq.Spec.Criteria)
	case "score":
		q.Type = Score
		var levels []string
		if err := json.Unmarshal(fq.Spec.Criteria, &levels); err != nil {
			t.Fatal(err)
		}
		for _, l := range levels {
			q.Options = append(q.Options, Option{Label: l})
		}
	case "noul":
		q.Type = Noul
		if len(fq.Spec.Criteria) > 0 {
			var m map[string]string
			_ = json.Unmarshal(fq.Spec.Criteria, &m)
			q.Options = []Option{{Description: m["false"]}, {Description: m["true"]}}
		}
	}
	return q
}

func orderedObject(t testing.TB, raw json.RawMessage) []Option {
	t.Helper()
	dec := json.NewDecoder(bytesReader(raw))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	var out []Option
	for dec.More() {
		k, _ := dec.Token()
		var v string
		if err := dec.Decode(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, Option{Label: k.(string), Description: v})
	}
	return out
}

func newEncoder(t testing.TB) (*tokenizer.Tokenizer, Specials, Config) {
	t.Helper()
	dir := modelDir(t)
	tk, err := tokenizer.Load(filepath.Join(dir, "tokenizer.json"))
	if err != nil {
		t.Fatal(err)
	}
	sp, err := LookupSpecials(tk)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	return tk, sp, cfg
}

// TestSequenceParity: token ids and marker positions must equal laya's
// build_sequence exactly for every fixture question.
func TestSequenceParity(t *testing.T) {
	tk, sp, cfg := newEncoder(t)
	n := 0
	for ci, c := range loadFixtures(t) {
		state := EncodeState(tk, c.State)
		for _, qid := range sortedKeys(c.Questions) {
			fq := c.Questions[qid]
			seq, err := BuildSequence(tk, sp, cfg, state, fq.question(t))
			if err != nil {
				t.Fatalf("case %d %s: %v", ci, qid, err)
			}
			if !slices.Equal(seq.IDs, fq.IDs) {
				j := 0
				for j < len(seq.IDs) && j < len(fq.IDs) && seq.IDs[j] == fq.IDs[j] {
					j++
				}
				t.Errorf("case %d %s: ids differ at %d (len %d vs %d)", ci, qid, j, len(seq.IDs), len(fq.IDs))
			}
			if !slices.Equal(seq.Markers, fq.Markers) {
				t.Errorf("case %d %s: markers %v, want %v", ci, qid, seq.Markers, fq.Markers)
			}
			n++
		}
	}
	if n < 20 {
		t.Fatalf("only %d fixture questions", n)
	}
}

// TestCalibrationParity: from the reference logits, temperature and
// probabilities match laya's decode.
func TestCalibrationParity(t *testing.T) {
	_, _, cfg := newEncoder(t)
	for ci, c := range loadFixtures(t) {
		for qid, fq := range c.Questions {
			q := fq.question(t)
			if got := cfg.TemperatureFor(q.Type, len(fq.Logits)); math.Abs(got-fq.Temperature) > 1e-12 {
				t.Errorf("case %d %s: temperature %v, want %v", ci, qid, got, fq.Temperature)
			}
			p := cfg.Calibrate(q.Type, fq.Logits)
			for i := range p {
				if d := math.Abs(p[i] - fq.Probabilities[i]); d > 1e-6 {
					t.Errorf("case %d %s: p[%d] = %v, want %v (|Δ| %g)", ci, qid, i, p[i], fq.Probabilities[i], d)
				}
			}
		}
	}
}

func TestTemperatureBucketsAndClamp(t *testing.T) {
	cfg := Config{
		Temperature:          []float64{1.5, 1.2, 1.9},
		TemperatureByOptions: map[string]float64{"choice:3-5": 1.7, "choice:11+": 0.1, "choice:2": math.NaN()},
		TemperatureClamp:     []float64{0.5, 5},
	}
	for _, tc := range []struct {
		q    QType
		k    int
		want float64
	}{
		{Choice, 4, 1.7}, {Choice, 7, 1.5}, {Choice, 12, 0.5}, {Choice, 2, 1}, {Score, 4, 1.2}, {Noul, 2, 1.9},
	} {
		if got := cfg.TemperatureFor(tc.q, tc.k); got != tc.want {
			t.Errorf("TemperatureFor(%s, %d) = %v, want %v", tc.q, tc.k, got, tc.want)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
