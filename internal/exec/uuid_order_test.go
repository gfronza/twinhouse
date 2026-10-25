package exec

import (
	"testing"

	"dynamodb-eventstream/internal/chtype"
	"dynamodb-eventstream/internal/exec/aggstate"
)

// UUIDs order as ClickHouse's UInt128 (high half = last 8 bytes), not as
// text: A < E < C < B < F < D below (verified against ClickHouse 25.8).
const (
	uuidA = "00000000-0000-0001-0000-000000000000"
	uuidB = "00000000-0000-0000-0000-000000000001"
	uuidC = "10000000-0000-0000-0000-000000000000"
	uuidD = "00000000-0000-0000-ff00-000000000000"
	uuidE = "00000000-0001-0000-0000-000000000000"
	uuidF = "00000000-0000-0000-0100-000000000000"
)

func TestUUIDOrderExpressions(t *testing.T) {
	te := newTestEnv(t)
	te.expectRows(`SELECT toUUID('`+uuidA+`') < toUUID('`+uuidB+`'), toUUID('`+uuidC+`') > toUUID('`+uuidB+`'), toUUID('`+uuidA+`') < '`+uuidB+`', '`+uuidB+`' > toUUID('`+uuidA+`')`, "1\t0\t1\t1")
	te.expectRows(`SELECT u FROM (SELECT toUUID(arrayJoin(['`+uuidB+`','`+uuidD+`','`+uuidA+`','`+uuidF+`','`+uuidC+`','`+uuidE+`'])) AS u) ORDER BY u`,
		uuidA+"\n"+uuidE+"\n"+uuidC+"\n"+uuidB+"\n"+uuidF+"\n"+uuidD)
	te.expectRows(`SELECT min(u), max(u), argMin(n, u), argMax(n, u) FROM (SELECT arrayJoin([(1,'`+uuidA+`'),(2,'`+uuidB+`'),(3,'`+uuidC+`'),(4,'`+uuidD+`')]) AS t, t.1 AS n, toUUID(t.2) AS u)`,
		uuidA+"\t"+uuidD+"\t1\t4")
	te.expectRows(`SELECT greatest(toUUID('`+uuidA+`'), toUUID('`+uuidB+`')), least(toUUID('`+uuidA+`'), toUUID('`+uuidB+`'))`, uuidB+"\t"+uuidA)
	te.expectRows(`SELECT [toUUID('`+uuidA+`')] < [toUUID('`+uuidB+`')], (toUUID('`+uuidA+`'), 1) < (toUUID('`+uuidB+`'), 1)`, "1\t1")
}

func TestUUIDOrderTableAndRollup(t *testing.T) {
	te := newTestEnv(t)
	te.exec(`CREATE TABLE uo (g UInt8, u UUID, n UInt32) ENGINE = MergeTree ORDER BY (g, u)`)
	te.exec(`INSERT INTO uo VALUES (1,'` + uuidA + `',1),(1,'` + uuidB + `',2),(1,'` + uuidC + `',3),(1,'` + uuidD + `',4),(2,'` + uuidB + `',5)`)
	te.expectRows(`SELECT count() FROM uo WHERE g = 1 AND u < '`+uuidB+`'`, "2")
	te.expectRows(`SELECT n FROM uo WHERE g = 1 AND u BETWEEN '`+uuidC+`' AND '`+uuidB+`' ORDER BY n`, "2\n3")
	te.expectRows(`SELECT n FROM uo WHERE g = 1 AND u > '`+uuidA+`' ORDER BY u LIMIT 2`, "3\n2")
	te.expectRows(`SELECT g, min(u), max(u) FROM uo GROUP BY g ORDER BY g`, "1\t"+uuidA+"\t"+uuidD+"\n2\t"+uuidB+"\t"+uuidB)

	te.exec(`CREATE TABLE uo_src (g UInt8, id UInt32, u UUID) ENGINE = MergeTree ORDER BY (g, id)`)
	te.exec(`CREATE TABLE uo_roll (g UInt8, lo SimpleAggregateFunction(min, UUID), hi SimpleAggregateFunction(max, UUID), hi_id AggregateFunction(argMax, UInt32, UUID)) ENGINE = AggregatingMergeTree ORDER BY g`)
	te.exec(`CREATE MATERIALIZED VIEW uo_mv TO uo_roll AS SELECT g, min(u) AS lo, max(u) AS hi, argMaxState(id, u) AS hi_id FROM uo_src GROUP BY g`)
	te.exec(`INSERT INTO uo_src VALUES (1, 1, '` + uuidD + `')`)
	te.exec(`INSERT INTO uo_src VALUES (1, 2, '` + uuidA + `')`)
	te.exec(`INSERT INTO uo_src VALUES (1, 3, '` + uuidB + `'), (1, 4, '` + uuidC + `')`)
	te.expectRows(`SELECT g, min(lo), max(hi), argMaxMerge(hi_id) FROM uo_roll GROUP BY g`, "1\t"+uuidA+"\t"+uuidD+"\t1")
}

func TestReplacesTypedUUID(t *testing.T) {
	// Text order says B < A; UInt128 order says A < B.
	if !aggstate.ReplacesTyped(uuidA, uuidB, chtype.TUUID, true) {
		t.Error("min: A should replace B")
	}
	if !aggstate.ReplacesTyped(uuidB, uuidA, chtype.TUUID, false) {
		t.Error("max: B should replace A")
	}
	if aggstate.ReplacesTyped(uuidB, uuidA, chtype.TUUID, true) {
		t.Error("min: B must not replace A")
	}
	if aggstate.CompareTyped(uuidA, uuidB, chtype.TUUID) >= 0 || aggstate.OrderKey(chtype.TUUID) == nil {
		t.Error("CompareTyped/OrderKey must order UUIDs as UInt128")
	}
}
