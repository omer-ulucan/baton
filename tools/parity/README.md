# Parity tooling (offline, Python)

Scripts that produce reference data for the Go decision layer. Python is
never a runtime dependency of Baton; these run on a developer machine and
their output is committed under `internal/laya/testdata/`.

```sh
python3 -m venv tools/parity/.venv
tools/parity/.venv/bin/pip install -r tools/parity/requirements.txt
MODEL=~/.cache/baton/models/42e2a6e3b3708c8ff5f61f4aa75c314b1449199b
tools/parity/.venv/bin/python -I tools/parity/inspect_graph.py $MODEL/onnx/model.onnx
tools/parity/.venv/bin/python -I tools/parity/tokenizer_corpus.py $MODEL/tokenizer.json out.json
```

- `inspect_graph.py` prints the ONNX graph's inputs, outputs and opset, so
  tensor names and shapes are read from the model, not guessed.
- `tokenizer_corpus.py` encodes a fixed corpus with Hugging Face
  `tokenizers` (`add_special_tokens=False`, as laya does) for tokenizer
  parity checks.
