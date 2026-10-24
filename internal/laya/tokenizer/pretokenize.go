package tokenizer

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// preTokenize splits text the way GPT-2's byte-level pre-tokenizer does,
// with the pattern
//
//	's|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+
//
// matched repeatedly from left to right. Go's RE2 has no lookahead, so the
// pattern is hand-coded; the alternatives are tried in order, each greedy,
// exactly as a backtracking engine does. The pattern covers every rune, so
// the matches tile the text.
func preTokenize(s string) []string {
	var words []string
	for i := 0; i < len(s); {
		n := matchAt(s, i)
		words = append(words, s[i:i+n])
		i += n
	}
	return words
}

var contractions = []string{"'s", "'t", "'re", "'ve", "'m", "'ll", "'d"}

// matchAt returns the length of the match starting at byte i (always > 0).
func matchAt(s string, i int) int {
	rest := s[i:]
	for _, c := range contractions {
		if strings.HasPrefix(rest, c) {
			return len(c)
		}
	}
	for _, class := range []func(rune) bool{isLetter, isNumber, isOther} {
		if n := optSpaceRun(rest, class); n > 0 {
			return n
		}
	}
	// \s+(?!\S): the whitespace run, minus its last rune when a
	// non-space follows (backtracking until the lookahead holds).
	run := runLen(rest, isWhitespace)
	if run > 0 {
		if run == len(rest) {
			return run
		}
		_, last := utf8.DecodeLastRuneInString(rest[:run])
		if run-last > 0 {
			return run - last
		}
		return run // \s+
	}
	// Unreachable: every rune is whitespace, a letter, a number or other.
	_, size := utf8.DecodeRuneInString(rest)
	return size
}

// optSpaceRun matches " ?class+".
func optSpaceRun(s string, class func(rune) bool) int {
	if strings.HasPrefix(s, " ") {
		if n := runLen(s[1:], class); n > 0 {
			return 1 + n
		}
	}
	return runLen(s, class)
}

func runLen(s string, class func(rune) bool) int {
	n := 0
	for n < len(s) {
		r, size := utf8.DecodeRuneInString(s[n:])
		if !class(r) {
			break
		}
		n += size
	}
	return n
}

func isLetter(r rune) bool { return unicode.IsLetter(r) }
func isNumber(r rune) bool { return unicode.IsNumber(r) }
func isOther(r rune) bool  { return !isWhitespace(r) && !isLetter(r) && !isNumber(r) }

// isWhitespace is Unicode White_Space, which is what \s and Rust's
// char::is_whitespace match.
func isWhitespace(r rune) bool { return unicode.Is(unicode.White_Space, r) }
