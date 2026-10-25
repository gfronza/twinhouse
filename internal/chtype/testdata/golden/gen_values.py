from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
rows = []
for line in open("values_cases.txt").read().split("\n"):
    if not line.strip(): continue
    t, v = line.split(" ||| ")
    if v.startswith("PY:"): v = eval(v[3:])
    qb("DROP TABLE IF EXISTS tv")
    r = qb("CREATE TABLE tv (x %s) ENGINE=Memory" % t)
    assert not r, (t, r)
    r = qb("INSERT INTO tv VALUES (%s)" % v)
    if r.startswith(b"ERR"):
        code = int(r.split(b".")[0][len("ERR Code: "):]); want = b""
    else:
        out = qb("SELECT x FROM tv FORMAT Values")
        code = 0; want = out[1:-1]
    rows.append((t, v, want, code))
with open(OUT_DIR + "/golden_values_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// valuesGolden: INSERT INTO t (x <typ>) VALUES (<in>); want is the stored value in Values form, or code the error code.\n")
    f.write("var valuesGolden = []valuesCase{\n")
    for t, v, want, code in rows:
        f.write("\t{typ: %s, in: %s, want: %s, code: %d},\n" % (golit(t), golit(v), golit(want), code))
    f.write("}\n")
print(len(rows))
