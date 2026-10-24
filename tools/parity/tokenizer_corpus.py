"""Reference token ids for tokenizer parity (offline tool, never runtime).

Usage: tokenizer_corpus.py TOKENIZER_JSON OUT_JSON

Encodes a fixed corpus with Hugging Face `tokenizers` (the library behind
laya's PreTrainedTokenizerFast), add_special_tokens=False as laya does.
"""
import json
import sys

from tokenizers import Tokenizer

STACK = """goroutine 17 [running]:
main.(*Ledger).Post(0xc000123456, {0x10, 0x20})
\t/app/ledger/post.go:88 +0x1f4
created by main.run in goroutine 1
\t/app/main.go:42 +0x9c"""

PY_TRACE = """Traceback (most recent call last):
  File "/srv/app/payments.py", line 214, in charge
    resp = client.post(url, json=body, timeout=5)
  File "/usr/lib/python3.12/site-packages/httpx/_client.py", line 1145, in post
    return self.request("POST", url, json=json)
httpx.ConnectTimeout: timed out after 5.0s"""

CORPUS = [
    "",
    " ",
    "hello world",
    "choice question: How should this task failure be handled?",
    " retry: a transient error that will likely succeed if tried again immediately",
    " retry_with_backoff: a transient error caused by load, rate limits or a dependency",
    " level 0: low",
    " false: no, the statement does not hold",
    "score question: How urgent is this task?",
    "connection reset by peer",
    "ERROR: duplicate key value violates unique constraint \"orders_pkey\"",
    "pq: deadlock detected (SQLSTATE 40P01)",
    "429 Too Many Requests: retry after 30s",
    "context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
    STACK,
    PY_TRACE,
    "a  b   c    d     e      f       g        h",
    " " * 30 + "deeply indented",
    "tab\tseparated\tvalues\n\nand\r\nnewlines",
    "trailing spaces   ",
    json.dumps({"error": {"type": "TimeoutError", "message": "upstream timed out", "attempt": 3, "max_attempts": 5}}, ensure_ascii=False),
    "Café vs Café (NFC must merge the combining accent)",
    "ＡＢＣ fullwidth and Ⅷ roman numeral",
    "emoji \U0001F389\U0001F525 and ❤️",
    "中文错误：连接超时",
    "हिन्दी त्रुटि",
    "Español: la conexión falló",
    "|||IP_ADDRESS||| and |||EMAIL_ADDRESS||| placeholders",
    "user 10.0.0.1 at admin@example.com",
    "<|padding|> <|endoftext|> literal special-looking text",
    "[CLS] [SEP] [MASK] [PAD] [UNK] as text",
    "numbers 1234567890 3.14159 -42 1e-9 0xdeadbeef",
    "punctuation!!! ??? ... --- *** ### @@@ $$$ %%% ^^^ &&&",
    "can't won't shouldn't it's I'm we're they've",
    "x" * 300,
    ("The quick brown fox jumps over the lazy dog. " * 40).strip(),
]


def main():
    tok = Tokenizer.from_file(sys.argv[1])
    cases = [{"text": t, "ids": tok.encode(t, add_special_tokens=False).ids} for t in CORPUS]
    with open(sys.argv[2], "w", encoding="utf-8") as f:
        json.dump({"tokenizers_version": __import__("tokenizers").__version__, "cases": cases}, f, ensure_ascii=False, indent=1)
    print("wrote", len(cases), "cases")


if __name__ == "__main__":
    main()
