from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
rows = []
for line in open("sup_cases.txt").read().strip().split("\n"):
    ts = [x.strip() for x in line.split("|")]
    vals = ", ".join("defaultValueOfTypeName(%s)" % lit(t) if t != "Nullable(Nothing)" else "NULL" for t in ts)
    r = qb("SELECT toTypeName([%s]) FORMAT TSVRaw" % vals, {"print_pretty_type_names": "0"})
    if r.startswith(b"ERR"):
        code = int(r.split(b".")[0][len("ERR Code: "):]); want = b""
    else:
        code = 0; want = r.rstrip(b"\n")
        assert want.startswith(b"Array(") and want.endswith(b")"); want = want[6:-1]
    rows.append((ts, want, code))
with open(OUT_DIR + "/golden_supertype_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// supertypeGolden: toTypeName([v1, v2, ...]) for values of the given types (Array stripped), or the error code.\n")
    f.write("var supertypeGolden = []supertypeCase{\n")
    for ts, want, code in rows:
        f.write("\t{in: []string{%s}, want: %s, code: %d},\n" % (", ".join(golit(t) for t in ts), golit(want), code))
    f.write("}\n")
print(len(rows))
