"""Differential corpus for tokenizer parity (offline tool, never runtime).

Usage: tokenizer_fuzz.py TOKENIZER_JSON OUT_JSON_GZ [N] [SEED]

Generates N strings from a fixed seed (default 3000, 20261024), designed
to hit every stage of the tokenizer.json pipeline, and records the ids
Hugging Face tokenizers produces with add_special_tokens=False.
"""
import gzip
import json
import random
import sys

from tokenizers import Tokenizer

import tokenizer_corpus

WHITESPACE = [" ", "\t", "\n", "\r", "\x0b", "\x0c", "\x85", "\xa0", " ", " ", " ",
              " ", " ", " ", " ", " ", " ", "　"]
NOT_WHITESPACE_BUT_ODD = ["​", "‍", "﻿", "᠎", "\x00", "\x7f"]
LETTERS = list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ") + list("éñüßøåçÉÑÜ")
DIGITS = list("0123456789") + ["٣", "५", "Ⅷ", "½", "①"]
PUNCT = list("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~") + ["—", "…", "«", "“", "’"]
COMBINING = ["́", "̈", "̧", "̣", "⃝"]
SCRIPTS = ["中文", "日本語", "한국어", "각", "العربية",
           "हिन्दी", "ไทย", "Русский", "עברית"]
EMOJI = ["\U0001F389", "\U0001F525", "❤️", "\U0001F44D\U0001F3FD", "\U0001F469‍\U0001F4BB",
         "\U0001F1F9\U0001F1F7", "©", "\U0001F600"]
CONTRACTIONS = ["'s", "'t", "'re", "'ve", "'m", "'ll", "'d", "'S", "'RE", "'LL", "’s", "'x"]
ADDED = ["|||IP_ADDRESS|||", "|||EMAIL_ADDRESS|||", "|||PHONE_NUMBER|||", "[MASK]", "[CLS]", "[SEP]", "[PAD]",
         "[UNK]", "<|endoftext|>", "<|padding|>", "[unused0]", "[unused42]", "|||IP_ADD", "[MAS", "[mask]"]
TEMPLATES = [
    "{svc}: dial tcp {ip}:{port}: connect: connection refused",
    "ERROR {code}: {msg} (attempt {n}/{m})",
    "panic: runtime error: index out of range [{n}] with length {m}\n\ngoroutine {n} [running]:\nmain.{fn}(...)\n\t/app/{fn}.go:{port} +0x{hex}",
    "Traceback (most recent call last):\n  File \"/srv/{fn}.py\", line {port}, in {fn}\n    raise {exc}(\"{msg}\")\n{exc}: {msg}",
    "{exc}: {msg}\n    at {svc}.{fn}({fn}.java:{port})\n    at java.base/java.lang.Thread.run(Thread.java:{n})",
    "HTTP {code} from {svc}: {{\"error\": \"{msg}\", \"retry_after\": {n}}}",
    "pq: {msg} (SQLSTATE {sqlstate})",
    "  {msg}  \t {msg}\r\n{msg}   ",
]
FILL = {
    "svc": ["payments", "ledger-api", "inventory_svc", "S3", "stripe.com", "auth"],
    "ip": ["10.0.0.12", "192.168.1.1", "::1", "|||IP_ADDRESS|||"],
    "port": ["443", "5432", "8080", "88"],
    "code": ["429", "500", "503", "E_TIMEOUT", "404"],
    "msg": ["timed out", "deadlock detected", "card declined", "rate limit exceeded", "no space left on device",
            "invalid input syntax for type uuid: \"abc\"", "context deadline exceeded", "Café not found", "连接超时"],
    "n": ["1", "3", "17", "4096"], "m": ["5", "10", "3"],
    "fn": ["chargeCard", "post_ledger", "Reserve", "run"],
    "hex": ["1f4", "9c", "deadbeef"],
    "exc": ["TimeoutError", "ValueError", "java.net.SocketTimeoutException", "KeyError"],
    "sqlstate": ["40P01", "23505", "57014"],
}


def atom(r):
    pool = r.random()
    if pool < 0.22:
        return r.choice(WHITESPACE) * r.choice([1, 1, 1, 2, 3, 5, 8, 17, 25, 31])
    if pool < 0.42:
        return "".join(r.choice(LETTERS) for _ in range(r.randint(1, 9)))
    if pool < 0.50:
        return "".join(r.choice(DIGITS) for _ in range(r.randint(1, 6)))
    if pool < 0.60:
        return "".join(r.choice(PUNCT) for _ in range(r.randint(1, 4)))
    if pool < 0.66:
        return r.choice(LETTERS) + r.choice(COMBINING)
    if pool < 0.72:
        return r.choice(SCRIPTS)
    if pool < 0.77:
        return r.choice(EMOJI)
    if pool < 0.83:
        return r.choice(CONTRACTIONS)
    if pool < 0.92:
        return r.choice(ADDED)
    return r.choice(NOT_WHITESPACE_BUT_ODD)


def template(r):
    t = r.choice(TEMPLATES)
    out = t
    for k, vs in FILL.items():
        while "{" + k + "}" in out:
            out = out.replace("{" + k + "}", r.choice(vs), 1)
    return out.replace("{{", "{").replace("}}", "}")


def sample(r):
    kind = r.random()
    if kind < 0.35:
        return template(r)
    if kind < 0.9:
        return "".join(atom(r) for _ in range(r.randint(1, 25)))
    return "".join(r.choice([template(r), atom(r)]) for _ in range(r.randint(20, 60)))[:2000]


def main():
    tok = Tokenizer.from_file(sys.argv[1])
    n = int(sys.argv[3]) if len(sys.argv) > 3 else 3000
    seed = int(sys.argv[4]) if len(sys.argv) > 4 else 20261024
    r = random.Random(seed)
    texts = list(tokenizer_corpus.CORPUS) + [sample(r) for _ in range(n)]
    cases = [{"text": t, "ids": tok.encode(t, add_special_tokens=False).ids} for t in texts]
    meta = {"tokenizers_version": __import__("tokenizers").__version__, "seed": seed, "n": n}
    with gzip.open(sys.argv[2], "wt", encoding="utf-8") as f:
        json.dump({"meta": meta, "cases": cases}, f, ensure_ascii=False)
    print("wrote", len(cases), "cases", meta)


if __name__ == "__main__":
    main()
