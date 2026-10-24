package tokenizer

import (
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// tokenizerPath finds the pinned tokenizer.json: $BATON_TOKENIZER_JSON, or
// the default model cache. Model files are never committed.
func tokenizerPath(t *testing.T) string {
	t.Helper()
	if p := os.Getenv("BATON_TOKENIZER_JSON"); p != "" {
		return p
	}
	home, _ := os.UserHomeDir()
	p := filepath.Join(home, ".cache", "baton", "models", "42e2a6e3b3708c8ff5f61f4aa75c314b1449199b", "tokenizer.json")
	if _, err := os.Stat(p); err != nil {
		t.Skipf("tokenizer.json not found at %s; run `baton model fetch --small` or set BATON_TOKENIZER_JSON", p)
	}
	return p
}

type parityCase struct {
	Text string `json:"text"`
	IDs  []int  `json:"ids"`
}

// loadCases reads the committed corpus, or $BATON_TOKENIZER_PARITY to
// check a larger or differently seeded one.
func loadCases(t *testing.T) []parityCase {
	t.Helper()
	path := "testdata/parity.json.gz"
	if p := os.Getenv("BATON_TOKENIZER_PARITY"); p != "" {
		path = p
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Cases []parityCase `json:"cases"`
	}
	if err := json.NewDecoder(gz).Decode(&data); err != nil {
		t.Fatal(err)
	}
	return data.Cases
}

// TestParityWithHuggingFace compares every case of the differential corpus
// (tools/parity/tokenizer_fuzz.py) with the ids Hugging Face tokenizers
// produced. Any difference fails: the model only works on exact ids.
func TestParityWithHuggingFace(t *testing.T) {
	tk, err := Load(tokenizerPath(t))
	if err != nil {
		t.Fatal(err)
	}
	cases := loadCases(t)
	if len(cases) < 3000 {
		t.Fatalf("only %d parity cases", len(cases))
	}
	failed := 0
	for i, c := range cases {
		got := tk.Encode(c.Text)
		if slices.Equal(got, c.IDs) || len(got)+len(c.IDs) == 0 {
			continue
		}
		failed++
		if failed <= 10 {
			j := 0
			for j < len(got) && j < len(c.IDs) && got[j] == c.IDs[j] {
				j++
			}
			t.Errorf("case %d %q: first difference at token %d\n want %v\n got  %v", i, short(c.Text), j, around(c.IDs, j), around(got, j))
		}
	}
	if failed > 0 {
		t.Fatalf("%d of %d cases differ from Hugging Face", failed, len(cases))
	}
}

func TestSpecialTokenIDs(t *testing.T) {
	tk, err := Load(tokenizerPath(t))
	if err != nil {
		t.Fatal(err)
	}
	for tok, want := range map[string]int{"[CLS]": 50281, "[SEP]": 50282, "[MASK]": 50284, "[PAD]": 50283} {
		if got, ok := tk.TokenID(tok); !ok || got != want {
			t.Errorf("%s = %d (%t), want %d", tok, got, ok, want)
		}
	}
}

func TestRejectsUnsupportedFiles(t *testing.T) {
	for name, body := range map[string]string{
		"wordpiece":     `{"model":{"type":"WordPiece"},"normalizer":{"type":"NFC"},"pre_tokenizer":{"type":"ByteLevel","use_regex":true}}`,
		"no normalizer": `{"model":{"type":"BPE","vocab":{},"merges":[]},"pre_tokenizer":{"type":"ByteLevel","use_regex":true}}`,
		"prefix space":  `{"model":{"type":"BPE","vocab":{},"merges":[]},"normalizer":{"type":"NFC"},"pre_tokenizer":{"type":"ByteLevel","add_prefix_space":true,"use_regex":true}}`,
		"rstrip token":  `{"added_tokens":[{"id":1,"content":"x","rstrip":true}],"model":{"type":"BPE","vocab":{},"merges":[]},"normalizer":{"type":"NFC"},"pre_tokenizer":{"type":"ByteLevel","use_regex":true}}`,
		"bad merge":     `{"model":{"type":"BPE","vocab":{"a":0},"merges":[["a","b"]]},"normalizer":{"type":"NFC"},"pre_tokenizer":{"type":"ByteLevel","use_regex":true}}`,
	} {
		if _, err := Parse([]byte(body)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPreTokenizeLookahead(t *testing.T) {
	// \s+(?!\S): a whitespace run before a word leaves its last space to
	// the word; at the end of the text it is taken whole.
	for in, want := range map[string][]string{
		"a  b":       {"a", " ", " b"},
		"x\n\ty":     {"x", "\n", "\t", "y"}, // " ?" is a literal space, not any whitespace
		"end   ":     {"end", "   "},
		"it's done":  {"it", "'s", " done"},
		" 42 apples": {" 42", " apples"},
		"\n\n":       {"\n\n"},
	} {
		if got := preTokenize(in); !slices.Equal(got, want) {
			t.Errorf("preTokenize(%q) = %q, want %q", in, got, want)
		}
	}
}

func BenchmarkEncodeStackTrace(b *testing.B) {
	home, _ := os.UserHomeDir()
	tk, err := Load(filepath.Join(home, ".cache", "baton", "models", "42e2a6e3b3708c8ff5f61f4aa75c314b1449199b", "tokenizer.json"))
	if err != nil {
		b.Skip(err)
	}
	text := "panic: runtime error: index out of range [5] with length 3\n\ngoroutine 1 [running]:\nmain.charge(...)\n\t/app/charge.go:88 +0x1f4\n"
	b.ReportAllocs()
	for b.Loop() {
		tk.Encode(text)
	}
}

func short(s string) string {
	if r := []rune(s); len(r) > 60 {
		return string(r[:60]) + "..."
	}
	return s
}

func around(s []int, j int) []int {
	return s[max(0, j-3):min(len(s), j+6)]
}
