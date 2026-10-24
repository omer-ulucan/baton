"""Golden fixtures for Baton's Go decision layer (offline tool, never runtime).

Usage: laya_fixtures.py MODEL_DIR OUT_JSON

MODEL_DIR is the pinned onnx-community/laya-ONNX download (config.json,
tokenizer.json, tokenizer_config.json, onnx/model.onnx + model.onnx_data).
The script runs laya 0.3.21's own ONNXAgent on exactly those files and
records, per question: token ids and marker positions (laya.common.
build_sequence), raw logits, the temperature applied, and unrounded
calibrated probabilities. Baton's Go port must reproduce ids and markers
exactly and probabilities within 1e-4.
"""
import json
import os
import shutil
import sys
import tempfile

import numpy as np

import laya
from laya.common import QTYPES, build_sequence, collate_items, temp_bucket
from laya.onnx_agent import ONNXAgent

# Baton's two decision questions, as internal/decision builds them.
TRIAGE = {
    "type": "choice",
    "instructions": "A task in a workflow failed with the error below. How should the orchestrator handle this failure?",
    "criteria": {
        "retry": "a transient glitch (network blip, connection reset) that should succeed if run again right away",
        "retry_with_backoff": "temporary overload or unavailability (timeouts, rate limits, 429/503, locks, deadlocks) that needs time before retrying",
        "dead_letter": "a permanent error that retrying cannot fix (invalid input, validation, not found, permission denied, declined)",
        "compensate": "a failure that leaves partial side effects or corrupt state, so completed steps must be undone",
        "escalate": "needs a human: data loss, security, disk full, misconfiguration, or anything unclear and risky",
    },
}
PRIORITY = {
    "type": "score",
    "instructions": "How urgent is this task?",
    "criteria": ["low: can wait hours or days", "normal: routine work", "high: user-facing or time-sensitive",
                 "critical: outage, revenue or data at risk"],
}

ERRORS = [
    "error_type: ConnectionResetError\nmessage: connection reset by peer\nhandler: payments.charge\nattempt: 1 of 5",
    "error_type: TimeoutError\nmessage: upstream timed out after 30s\nhandler: inventory.reserve\nattempt: 2 of 5",
    "error_type: HTTPError\nmessage: 429 Too Many Requests: retry after 30s\nhandler: email.send\nattempt: 1 of 3",
    "error_type: ValidationError\nmessage: invalid card number\nhandler: payments.charge\nattempt: 1 of 5",
    "error_type: PermissionError\nmessage: access denied for bucket invoices-prod\nhandler: storage.put\nattempt: 3 of 5",
    "error_type: OSError\nmessage: no space left on device\nhandler: reports.render\nattempt: 1 of 5",
    "error_type: LedgerError\nmessage: balance mismatch after partial post; ledger left inconsistent\nhandler: ledger.post\nattempt: 1 of 5",
    "error_type: pq.Error\nmessage: deadlock detected (SQLSTATE 40P01)\nhandler: orders.write\nattempt: 2 of 5",
    "error_type: KeyError\nmessage: 'customer_id'\nstack:\nTraceback (most recent call last):\n  File \"/srv/app/charge.py\", line 41, in run\n    cid = payload['customer_id']\nKeyError: 'customer_id'\nhandler: payments.charge\nattempt: 1 of 5",
    "error_type: panic\nmessage: runtime error: invalid memory address or nil pointer dereference\nstack:\ngoroutine 7 [running]:\nmain.(*Ledger).Post(0x0)\n\t/app/ledger.go:88 +0x1f4\nhandler: ledger.post\nattempt: 1 of 5",
]
DESCRIPTIONS = [
    "Send the weekly newsletter digest",
    "Refund a customer who was charged twice",
    "Rebuild the search index for the docs site",
    "Restore checkout after the payment provider outage",
    "Nightly analytics export",
]
MASK = "[MASK]"

CASES = (
    [{"state": e, "questions": {"triage": TRIAGE}} for e in ERRORS]
    + [{"state": d, "questions": {"priority": PRIORITY}} for d in DESCRIPTIONS]
    + [
        # laya's own edge cases (conversion/laya_reference.py): double spaces, a mask token in
        # the text, over-long options, many options exhausting the head budget, noul questions.
        {"state": "Hi,  we were   billed twice for March.\n\nPlease refund  the duplicate.",
         "questions": {
             "d": {"type": "choice", "instructions": "Which  department?",
                   "criteria": {"billing": "invoices, payments, refunds", "technical": "", "other": "everything else"}},
             "u": {"type": "score", "instructions": "How urgent is this?", "criteria": ["not urgent", "soon", "blocking"]},
             "r": {"type": "noul", "instructions": "Does the user ask for a refund?"},
             "c": {"type": "noul", "instructions": "Will they cancel?", "criteria": {"false": "They stay.", "true": "They leave."}}}},
        {"state": f"Text with a {MASK} token, émojis \U0001F389, and ünïcode — ok?",
         "questions": {"m": {"type": "choice", "instructions": f"Pick {MASK} one",
                             "criteria": {"a": "refund requested for a duplicate charge on the March invoice " * 6, "b": "short"}}}},
        {"state": "Many options case.",
         "questions": {"many": {"type": "choice", "instructions": "Which of these topics fits best? " * 8,
                                "criteria": {f"topic_{i}": f"a fairly long description of topic number {i} for budget" for i in range(14)}}}},
        {"state": "two options only", "questions": {"two": {"type": "choice", "instructions": "Yes or no?", "criteria": {"yes": "", "no": ""}}}},
        {"state": "seven options", "questions": {"seven": {"type": "choice", "instructions": "Which colour?",
                                                            "criteria": {c: "" for c in ["red", "orange", "yellow", "green", "blue", "indigo", "violet"]}}}},
        # A state far past max_len is truncated to the room left after the head.
        {"state": ("The service logged a long sequence of retries and timeouts. " * 120).strip(),
         "questions": {"triage": TRIAGE}},
    ]
)


def model_dir_for_agent(src):
    """laya's ONNXAgent expects the original repo layout; mirror the pinned files into it."""
    cfg = json.load(open(os.path.join(src, "config.json")))["laya"]
    tmp = tempfile.mkdtemp(prefix="laya-agent-")
    with open(os.path.join(tmp, "rl_agent_config.json"), "w") as f:
        json.dump({k: cfg[k] for k in ("max_len", "head_max_len", "temperature", "temperature_by_options", "encoder")}, f)
    os.makedirs(os.path.join(tmp, "tokenizer"))
    for name in ("tokenizer.json", "tokenizer_config.json"):
        shutil.copy(os.path.join(src, name), os.path.join(tmp, "tokenizer", name))
    return tmp, cfg


def main():
    src, out = sys.argv[1], sys.argv[2]
    tmp, cfg = model_dir_for_agent(src)
    agent = ONNXAgent(tmp, onnx_path=os.path.join(src, "onnx", "model.onnx"))
    tok = agent.tok
    max_len, head = cfg["max_len"], cfg["head_max_len"]

    fixtures = []
    for case in CASES:
        internal = {qid: ONNXAgent._to_internal(q) for qid, q in case["questions"].items()}
        ids_order = list(internal)
        items = agent._encode_state(case["state"], ids_order, internal, max_len, head)
        b = collate_items([items], tok.pad_token_id)
        logits, _ = agent.session.run(["logits", "act_logits"], {
            "input_ids": b["input_ids"].numpy().astype(np.int64),
            "attention_mask": b["attention_mask"].numpy().astype(np.int64),
            "marker_pos": b["marker_pos"].numpy().astype(np.int64),
            "marker_mask": b["marker_mask"].numpy().astype(bool),
            "qtype": b["qtype"].numpy().astype(np.int64),
        })
        questions = {}
        for r, qid in enumerate(ids_order):
            q = internal[qid]
            seq, markers = build_sequence(tok, case["state"], q, max_len, head)
            assert seq == items[r]["ids"] and markers == items[r]["markers"]
            k, qt = len(markers), QTYPES[q["t"]]
            t_scale = agent.temperature_by_options.get(temp_bucket(qt, k), agent.temperature[qt])
            z = logits[r, :k].astype(np.float64) / t_scale
            p = np.exp(z - z.max())
            p = p / p.sum()
            questions[qid] = {
                "spec": case["questions"][qid], "ids": seq, "markers": markers,
                "logits": [float(x) for x in logits[r, :k]], "temperature": float(t_scale),
                "probabilities": [float(x) for x in p],
            }
        fixtures.append({"state": case["state"], "questions": questions})
    meta = {"laya": laya.__version__ if hasattr(laya, "__version__") else "0.3.21",
            "onnxruntime": __import__("onnxruntime").__version__, "max_len": max_len, "head_max_len": head}
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "cases": fixtures}, f, ensure_ascii=False, indent=1)
    shutil.rmtree(tmp)
    print("wrote", sum(len(c["questions"]) for c in fixtures), "questions in", len(fixtures), "cases", meta)


if __name__ == "__main__":
    main()
