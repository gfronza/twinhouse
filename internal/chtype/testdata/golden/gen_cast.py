from ch import *
import os
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")
from golib import golit
rows = []
for line in open("cast_cases.txt").read().split("\n"):
    if not line.strip(): continue
    src, to = line.rsplit("|", 1)
    ftype = qb("SELECT toTypeName(%s) FORMAT TSVRaw" % src, {"print_pretty_type_names":"0"}).rstrip(b"\n")
    sval = qb("SELECT %s FORMAT Values" % src)[1:-1]
    totype = qb("SELECT toTypeName(CAST(defaultValueOfTypeName(%s), %s)) FORMAT TSVRaw" % (lit(to), lit(to)), {"print_pretty_type_names":"0"}).rstrip(b"\n")
    assert not ftype.startswith(b"ERR") and not totype.startswith(b"ERR"), (line, ftype, totype)
    r = qb("SELECT CAST(%s AS %s) FORMAT Values" % (src, to))
    if r.startswith(b"ERR"):
        code = int(r.split(b".")[0][len("ERR Code: "):]); want = b""
    else:
        code = 0; want = r[1:-1]
    rows.append((ftype, sval, totype, want, code))
with open(OUT_DIR + "/golden_cast_data_test.go", "w") as f:
    f.write("// Code generated from ClickHouse 25.8 output by a probe script; DO NOT EDIT.\n\npackage chtype\n\n")
    f.write("// castGolden: CAST(<src value of type from> AS <to>) in ClickHouse; want is the Values rendering, or code the error code.\n")
    f.write("var castGolden = []castCase{\n")
    for ft, sv, tt, want, code in rows:
        f.write("\t{from: %s, src: %s, to: %s, want: %s, code: %d},\n" % (golit(ft), golit(sv), golit(tt), golit(want), code))
    f.write("}\n")
print(len(rows))
