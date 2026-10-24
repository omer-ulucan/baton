package tokenizer

// trie finds the longest added token starting at a byte offset. Scanning
// offsets left to right and taking the longest token at the first offset
// that has one is leftmost-longest matching, the semantics Hugging Face's
// Aho-Corasick matcher uses for added tokens.
type trie struct {
	next map[byte]*trie
	id   int
	end  bool
}

func newTrie() *trie { return &trie{} }

func (t *trie) empty() bool { return len(t.next) == 0 }

func (t *trie) add(s string, id int) {
	n := t
	for i := 0; i < len(s); i++ {
		if n.next == nil {
			n.next = map[byte]*trie{}
		}
		c, ok := n.next[s[i]]
		if !ok {
			c = &trie{}
			n.next[s[i]] = c
		}
		n = c
	}
	if !n.end { // the first definition of a duplicate wins
		n.end, n.id = true, id
	}
}

// longest returns the end offset and id of the longest token at s[i:].
func (t *trie) longest(s string, i int) (end, id int, ok bool) {
	n := t
	for j := i; j < len(s); j++ {
		c, found := n.next[s[j]]
		if !found {
			break
		}
		n = c
		if n.end {
			end, id, ok = j+1, n.id, true
		}
	}
	return end, id, ok
}
