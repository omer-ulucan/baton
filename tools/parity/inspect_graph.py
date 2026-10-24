"""Print an ONNX graph's inputs, outputs and opset. Offline tool only."""
import sys

import onnx

m = onnx.load(sys.argv[1], load_external_data=False)
names = {t: n for n, t in onnx.TensorProto.DataType.items()}


def show(kind, values):
    for v in values:
        t = v.type.tensor_type
        dims = [d.dim_param or d.dim_value for d in t.shape.dim]
        print(f"{kind:6} {v.name:16} {names[t.elem_type]:6} {dims}")


print("opset", [(o.domain or "ai.onnx", o.version) for o in m.opset_import])
print("producer", m.producer_name, m.producer_version)
show("input", m.graph.input)
show("output", m.graph.output)
