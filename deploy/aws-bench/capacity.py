#!/usr/bin/env python3
"""DynamoDB capacity per chbench step, from CloudWatch.

    capacity.py <chbench results JSON> <table> [<table> ...]
    capacity.py <chbench results JSON> --prefix <table prefix>

With --prefix, the tables are those CloudWatch has DynamoDB metrics for
(it keeps them two weeks), so tables dropped since the run still count.

CloudWatch sums ConsumedRead/WriteCapacityUnits and Read/WriteThrottleEvents
per table and minute; a
minute counts toward a step when it starts inside the step's window
(chbench -align-steps starts every step just after a minute boundary).
Calls the AWS CLI with the caller's AWS_PROFILE and AWS_REGION. Prices are
on-demand list prices per million request units; override them with
DDB_READ_PRICE and DDB_WRITE_PRICE.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

READ_PRICE = float(os.environ.get("DDB_READ_PRICE", "0.125"))
WRITE_PRICE = float(os.environ.get("DDB_WRITE_PRICE", "0.625"))


def ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def metric_data(tables, start, end):
    """{(metric, table): {minute: sum}} for capacity and throttle metrics."""
    queries, keys = [], {}
    for i, t in enumerate(tables):
        for m, short in (("ConsumedReadCapacityUnits", "r"), ("ConsumedWriteCapacityUnits", "w"),
                         ("ReadThrottleEvents", "rt"), ("WriteThrottleEvents", "wt")):
            qid = f"{short}{i}"
            keys[qid] = (short, t)
            queries.append({
                "Id": qid,
                "MetricStat": {
                    "Metric": {"Namespace": "AWS/DynamoDB", "MetricName": m,
                               "Dimensions": [{"Name": "TableName", "Value": t}]},
                    "Period": 60, "Stat": "Sum"},
            })
    out = {}
    for n in range(0, len(queries), 500):
        args = ["aws", "cloudwatch", "get-metric-data", "--output", "json",
                "--start-time", start.isoformat(), "--end-time", end.isoformat(),
                "--metric-data-queries", json.dumps(queries[n:n + 500])]
        token = None
        while True:
            res = json.loads(subprocess.check_output(args + (["--next-token", token] if token else [])))
            for r in res["MetricDataResults"]:
                d = out.setdefault(keys[r["Id"]], {})
                for t, v in zip(r["Timestamps"], r["Values"]):
                    d[ts(t)] = d.get(ts(t), 0) + v
            token = res.get("NextToken")
            if not token:
                break
    return out


def metric_tables(prefix):
    """Tables with a write-capacity metric whose name starts with prefix."""
    out = json.loads(subprocess.check_output([
        "aws", "cloudwatch", "list-metrics", "--output", "json", "--namespace", "AWS/DynamoDB",
        "--metric-name", "ConsumedWriteCapacityUnits"]))
    names = set()
    for m in out["Metrics"]:
        dims = {d["Name"]: d["Value"] for d in m["Dimensions"]}
        if set(dims) == {"TableName"} and dims["TableName"].startswith(prefix):
            names.add(dims["TableName"])
    return sorted(names)


def main():
    if len(sys.argv) < 3:
        sys.exit(__doc__)
    run = json.load(open(sys.argv[1]))
    if sys.argv[2] == "--prefix" and len(sys.argv) == 4:
        tables = metric_tables(sys.argv[3])
    else:
        tables = sys.argv[2:]
    steps = [(s["id"], ts(s["start"]), ts(s["end"])) for s in run.get("steps", [])]
    if not steps:
        sys.exit("no steps in the results (chbench older than -align-steps?)")
    start = min(s for _, s, _ in steps).replace(second=0, microsecond=0)
    end = max(e for _, _, e in steps) + timedelta(minutes=2)
    data = metric_data(tables, start, end)

    print(f"DynamoDB capacity per step ({len(tables)} tables; "
          f"${READ_PRICE}/M reads, ${WRITE_PRICE}/M writes on demand)\n")
    print("| Step | Read units | Write units | Throttled reads | Throttled writes | Cost |\n|---|--:|--:|--:|--:|--:|")
    total = [0.0, 0.0]
    hot = {}
    for sid, s, e in steps:
        lo = s.replace(second=0, microsecond=0)
        sums = {"r": 0.0, "w": 0.0, "rt": 0.0, "wt": 0.0}
        for (short, table), series in data.items():
            v = sum(v for t, v in series.items() if lo <= t < e)
            sums[short] += v
            if short in ("rt", "wt") and v:
                hot[table] = hot.get(table, 0) + v
        cost = sums["r"] / 1e6 * READ_PRICE + sums["w"] / 1e6 * WRITE_PRICE
        total[0] += sums["r"]
        total[1] += sums["w"]
        print(f"| {sid} | {sums['r']:,.0f} | {sums['w']:,.0f} | {sums['rt']:,.0f} | {sums['wt']:,.0f} | ${cost:.4f} |")
    cost = total[0] / 1e6 * READ_PRICE + total[1] / 1e6 * WRITE_PRICE
    print(f"| total | {total[0]:,.0f} | {total[1]:,.0f} | | | ${cost:.4f} |")
    if hot:
        print("\nThrottled requests by table:")
        for t, v in sorted(hot.items(), key=lambda x: -x[1]):
            print(f"- {t}: {v:,.0f}")
    now = datetime.now(timezone.utc)
    if now - max(e for _, _, e in steps) < timedelta(minutes=5):
        print("\nThe run ended under 5 minutes ago; CloudWatch may not have the last minutes yet.")


if __name__ == "__main__":
    main()
