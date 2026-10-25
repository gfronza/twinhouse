from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
rows = []
for line in open("params_cases.txt").read().split("\n"):
    if not line.strip(): continue
    t, v = line.split(" ||| ")
    if v.startswith("PY:"): v = eval(v[3:])
    r = qb("SELECT {x:%s} AS v FORMAT Values" % t, {"param_x": v})
    if r.startswith(b"ERR"):
        code = int(r.split(b".")[0][len("ERR Code: "):])
        rows.append((t, v, None, code))
    else:
        assert r[:1] == b"(" and r[-1:] == b")"
        rows.append((t, v, r[1:-1], 0))
with open(OUT_DIR + "/golden_params_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// paramGolden: SELECT {x:<typ>} with param_x=<in>; want is the Values rendering, or code the error code.\n")
    f.write("var paramGolden = []paramCase{\n")
    for t, v, want, code in rows:
        f.write("\t{typ: %s, in: %s, want: %s, code: %d},\n" % (golit(t), golit(v), golit(want or b""), code))
    f.write("}\n")
print(len(rows))
