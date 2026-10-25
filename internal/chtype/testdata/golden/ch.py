import os, sys, urllib.request, urllib.parse
def qb(sql, params=None):
    url = os.environ.get("CH_URL", "http://localhost:18126/")
    p = dict(params or {})
    if p: url += "?" + urllib.parse.urlencode(p)
    req = urllib.request.Request(url, data=sql.encode(), method="POST")
    try:
        with urllib.request.urlopen(req) as r: return r.read()
    except urllib.error.HTTPError as e:
        return b"ERR " + e.read().split(b"(version")[0].strip()
def q(sql, params=None, fmt=None):
    p = dict(params or {})
    if fmt: p["default_format"] = fmt
    return qb(sql, p).decode("utf-8","backslashreplace")
def lit(s): return "'" + s.replace("\\","\\\\").replace("'","\\'") + "'"
