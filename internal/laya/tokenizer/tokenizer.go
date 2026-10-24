// Package tokenizer is a pure-Go implementation of the Hugging Face
// tokenizer.json pipeline used by Laya's ModernBERT encoder: added-token
// extraction, NFC normalization, the byte-level GPT-2 pre-tokenizer and
// BPE. It supports exactly the components that tokenizer uses and refuses
// to load anything else, so an unsupported file fails loudly instead of
// tokenizing differently.
//
// Parity with the reference implementation is checked by a differential
// test against ids produced by Hugging Face tokenizers (tools/parity).
package tokenizer

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Tokenizer encodes text to token ids. It is safe for concurrent use.
type Tokenizer struct {
	vocab  map[string]int
	merges map[[2]int]merge // pair of symbol ids -> rank and merged id
	// Added tokens are extracted before the model runs: raw ones from the
	// input text, normalized ones from the NFC-normalized text.
	raw, normalized *trie
	byID            map[int]addedToken
	byteChar        [256]rune
	special         map[string]int
}

type merge struct {
	rank int
	id   int
}

type addedToken struct {
	ID         int    `json:"id"`
	Content    string `json:"content"`
	SingleWord bool   `json:"single_word"`
	LStrip     bool   `json:"lstrip"`
	RStrip     bool   `json:"rstrip"`
	Normalized bool   `json:"normalized"`
	Special    bool   `json:"special"`
}

type file struct {
	AddedTokens []addedToken `json:"added_tokens"`
	Normalizer  *struct {
		Type string `json:"type"`
	} `json:"normalizer"`
	PreTokenizer *struct {
		Type           string `json:"type"`
		AddPrefixSpace bool   `json:"add_prefix_space"`
		UseRegex       bool   `json:"use_regex"`
	} `json:"pre_tokenizer"`
	Model struct {
		Type                    string         `json:"type"`
		Vocab                   map[string]int `json:"vocab"`
		Merges                  [][2]string    `json:"merges"`
		Dropout                 *float64       `json:"dropout"`
		UnkToken                *string        `json:"unk_token"`
		ContinuingSubwordPrefix *string        `json:"continuing_subword_prefix"`
		EndOfWordSuffix         *string        `json:"end_of_word_suffix"`
		ByteFallback            bool           `json:"byte_fallback"`
		IgnoreMerges            bool           `json:"ignore_merges"`
	} `json:"model"`
}

// Load reads a tokenizer.json.
func Load(path string) (*Tokenizer, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse builds a tokenizer from tokenizer.json content.
func Parse(data []byte) (*Tokenizer, error) {
	var f file
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("tokenizer.json: %w", err)
	}
	if err := supported(&f); err != nil {
		return nil, fmt.Errorf("tokenizer.json: %w", err)
	}
	t := &Tokenizer{
		vocab: f.Model.Vocab, merges: make(map[[2]int]merge, len(f.Model.Merges)),
		raw: newTrie(), normalized: newTrie(), byID: map[int]addedToken{}, special: map[string]int{},
	}
	for rank, m := range f.Model.Merges {
		a, aok := t.vocab[m[0]]
		b, bok := t.vocab[m[1]]
		ab, abok := t.vocab[m[0]+m[1]]
		if !aok || !bok || !abok {
			return nil, fmt.Errorf("tokenizer.json: merge %d (%q %q) refers to tokens missing from the vocabulary", rank, m[0], m[1])
		}
		if _, dup := t.merges[[2]int{a, b}]; !dup {
			t.merges[[2]int{a, b}] = merge{rank: rank, id: ab}
		}
	}
	for _, at := range f.AddedTokens {
		if at.Content == "" {
			continue
		}
		t.byID[at.ID] = at
		if at.Special {
			t.special[at.Content] = at.ID
		}
		if at.Normalized && !at.Special {
			t.normalized.add(at.Content, at.ID)
		} else {
			t.raw.add(at.Content, at.ID)
		}
	}
	// Not every byte has a symbol: bytes that never occur in valid UTF-8
	// (0xC0, 0xC1, 0xF5-0xFF) are absent from GPT-NeoX-style vocabularies.
	t.byteChar = bytesToUnicode()
	return t, nil
}

func supported(f *file) error {
	switch {
	case f.Model.Type != "BPE":
		return fmt.Errorf("model %q unsupported, want BPE", f.Model.Type)
	case f.Model.Dropout != nil && *f.Model.Dropout != 0:
		return fmt.Errorf("BPE dropout unsupported")
	case f.Model.ContinuingSubwordPrefix != nil && *f.Model.ContinuingSubwordPrefix != "",
		f.Model.EndOfWordSuffix != nil && *f.Model.EndOfWordSuffix != "":
		return fmt.Errorf("subword prefix/suffix unsupported")
	case f.Model.ByteFallback || f.Model.IgnoreMerges:
		return fmt.Errorf("byte_fallback and ignore_merges unsupported")
	case f.Normalizer == nil || f.Normalizer.Type != "NFC":
		return fmt.Errorf("normalizer must be NFC")
	case f.PreTokenizer == nil || f.PreTokenizer.Type != "ByteLevel" || f.PreTokenizer.AddPrefixSpace || !f.PreTokenizer.UseRegex:
		return fmt.Errorf("pre_tokenizer must be ByteLevel with use_regex and no prefix space")
	}
	for _, at := range f.AddedTokens {
		if at.SingleWord || at.RStrip {
			return fmt.Errorf("added token %q: single_word and rstrip unsupported", at.Content)
		}
		if at.Special && at.Normalized {
			return fmt.Errorf("added token %q: normalized special tokens unsupported", at.Content)
		}
	}
	return nil
}

// TokenID returns the id of a special token such as "[CLS]".
func (t *Tokenizer) TokenID(content string) (int, bool) {
	id, ok := t.special[content]
	return id, ok
}

// Encode tokenizes text without adding special tokens (laya encodes with
// add_special_tokens=False and assembles sequences itself).
func (t *Tokenizer) Encode(text string) []int {
	var ids []int
	// 1. Added tokens that are not normalized are found in the raw text.
	for _, p := range t.split(text, t.raw) {
		if p.added {
			ids = append(ids, p.id)
			continue
		}
		// 2. The rest is normalized, then normalized added tokens are found.
		for _, q := range t.split(norm.NFC.String(p.text), t.normalized) {
			if q.added {
				ids = append(ids, q.id)
				continue
			}
			// 3. Pre-tokenize and run BPE on each word.
			for _, w := range preTokenize(q.text) {
				ids = t.bpe(ids, w)
			}
		}
	}
	return ids
}

type piece struct {
	text  string
	added bool
	id    int
}

// split cuts text around added tokens found by leftmost-longest matching,
// applying lstrip (absorb whitespace before the token) as Hugging Face's
// AddedVocabulary does.
func (t *Tokenizer) split(text string, tr *trie) []piece {
	if tr.empty() {
		return []piece{{text: text}}
	}
	var out []piece
	start := 0 // end of the previous piece
	for i := 0; i < len(text); {
		end, id, ok := tr.longest(text, i)
		if !ok {
			i++
			continue
		}
		mStart := i
		if t.byID[id].LStrip {
			s := mStart
			for s > start {
				r, size := utf8.DecodeLastRuneInString(text[:s])
				if !isWhitespace(r) {
					break
				}
				s -= size
			}
			mStart = s
		}
		if start < mStart {
			out = append(out, piece{text: text[start:mStart]})
		}
		out = append(out, piece{added: true, id: id})
		start, i = end, end
	}
	if start < len(text) {
		out = append(out, piece{text: text[start:]})
	}
	return out
}

// bpe appends the ids of one pre-tokenized word.
func (t *Tokenizer) bpe(ids []int, word string) []int {
	syms := make([]int, 0, len(word))
	for i := 0; i < len(word); i++ {
		// With no unk token, a symbol outside the vocabulary is dropped,
		// as Hugging Face's BPE does.
		if id, ok := t.vocab[string(t.byteChar[word[i]])]; ok {
			syms = append(syms, id)
		}
	}
	for len(syms) > 1 {
		best, at := merge{rank: -1}, -1
		for i := 0; i+1 < len(syms); i++ {
			m, ok := t.merges[[2]int{syms[i], syms[i+1]}]
			if ok && (best.rank < 0 || m.rank < best.rank) {
				best, at = m, i
			}
		}
		if at < 0 {
			break
		}
		syms[at] = best.id
		syms = append(syms[:at+1], syms[at+2:]...)
	}
	return append(ids, syms...)
}

// bytesToUnicode is GPT-2's reversible byte to printable-rune mapping.
func bytesToUnicode() [256]rune {
	var table [256]rune
	var direct []int
	for b := '!'; b <= '~'; b++ {
		direct = append(direct, int(b))
	}
	for b := 0xA1; b <= 0xAC; b++ {
		direct = append(direct, b)
	}
	for b := 0xAE; b <= 0xFF; b++ {
		direct = append(direct, b)
	}
	sort.Ints(direct)
	isDirect := map[int]bool{}
	for _, b := range direct {
		table[b] = rune(b)
		isDirect[b] = true
	}
	n := 0
	for b := 0; b < 256; b++ {
		if !isDirect[b] {
			table[b] = rune(256 + n)
			n++
		}
	}
	return table
}
