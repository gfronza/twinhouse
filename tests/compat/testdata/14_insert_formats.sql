-- Workload shape 6: loader-style inserts (JSONEachRow bodies, gzip, query in
-- URL + data in body, TSV/CSV/Values inputs, column subsets with defaults,
-- INSERT ... SELECT), read back in several formats.

CREATE TABLE ingest (
    tenant_id String,
    event_time DateTime,
    event_id String,
    amount UInt64 DEFAULT 0,
    price Float64 DEFAULT 1.25,
    note Nullable(String),
    tags Array(String) DEFAULT [],
    t3 DateTime64(3) DEFAULT toDateTime64('2024-01-01 00:00:00', 3),
    kind LowCardinality(String) DEFAULT 'view',
    flag Bool DEFAULT false,
    derived String DEFAULT concat(tenant_id, ':', event_id)
) ENGINE = MergeTree ORDER BY (tenant_id, event_time, event_id);

-- split-data
INSERT INTO ingest FORMAT JSONEachRow
{"tenant_id":"acme","event_time":"2024-05-01 10:00:00","event_id":"j1","amount":5,"price":0.5,"note":"he said \"hi\"\n\ttab","tags":["a","b"],"t3":"2024-05-01 10:00:00.123","kind":"click","flag":true}
{"tenant_id":"acme","event_time":"2024-05-01 10:00:01","event_id":"j2","note":null}
{"tenant_id":"acme","event_time":1714557602,"event_id":"j3","amount":"7","note":"unicode é ✓"}
{"event_id":"j4","tenant_id":"globex","event_time":"2024-05-01 10:00:03","amount":18446744073709551615,"tags":[]};

-- gzip
INSERT INTO ingest FORMAT JSONEachRow
{"tenant_id":"gz","event_time":"2024-05-02 00:00:00","event_id":"z1","amount":1}
{"tenant_id":"gz","event_time":"2024-05-02 00:00:01","event_id":"z2","amount":2,"note":"compressed"};

-- gzip
-- split-data
INSERT INTO ingest (tenant_id, event_time, event_id, amount) FORMAT JSONEachRow
{"tenant_id":"gz","event_time":"2024-05-02 00:00:02","event_id":"z3","amount":3};

INSERT INTO ingest (tenant_id, event_time, event_id, note) FORMAT TabSeparated
tsv	2024-05-03 00:00:00	t1	tab\there
tsv	2024-05-03 00:00:01	t2	\N
tsv	2024-05-03 00:00:02	t3	back\\slash nl\nend;

INSERT INTO ingest (tenant_id, event_time, event_id, amount, note) FORMAT CSV
csv,2024-05-04 00:00:00,c1,10,"quoted, with comma"
csv,"2024-05-04 00:00:01",c2,11,"double ""quote"""
csv,2024-05-04 00:00:02,c3,12,\N;

INSERT INTO ingest (tenant_id, event_time, event_id, amount) FORMAT CSVWithNames
"tenant_id","event_time","event_id","amount"
csvh,2024-05-05 00:00:00,h1,1;

INSERT INTO ingest (event_id, tenant_id, event_time, note) FORMAT TSVWithNames
event_id	tenant_id	event_time	note
n1	tsvh	2024-05-06 00:00:00	reordered columns;

INSERT INTO ingest (tenant_id, event_time, event_id, amount, tags) VALUES ('vals', '2024-05-07 00:00:00', 'v1', 1, ['x', 'it''s']), ('vals', '2024-05-07 00:00:01', 'v2', 2 + 3, splitByChar(',', 'p,q'));

-- param: t=params
-- param: id=p1
-- param: amt=42
INSERT INTO ingest (tenant_id, event_time, event_id, amount) VALUES ({t:String}, '2024-05-08 00:00:00', {id:String}, {amt:UInt64});

INSERT INTO ingest (tenant_id, event_time, event_id, amount, note)
SELECT 'copy', event_time + 86400, concat(event_id, '_c'), amount * 2, note FROM ingest WHERE tenant_id = 'acme';

INSERT INTO ingest (tenant_id, event_time, event_id) SELECT 'nums', toDateTime('2024-06-01 00:00:00') + number, toString(number) FROM numbers(5);

INSERT INTO ingest FORMAT Values ('fmtv', '2024-05-09 00:00:00', 'fv1', 3, 2.5, 'values fmt', ['q'], '2024-05-09 00:00:00.5', 'buy', true, 'explicit');

-- query: ingest_all
-- formats: TSVWithNamesAndTypes,JSONEachRow,CSVWithNames,JSONCompact,Native
SELECT * FROM ingest ORDER BY tenant_id, event_time, event_id;

-- query: ingest_counts
SELECT tenant_id, count(), sum(amount) FROM ingest GROUP BY tenant_id ORDER BY tenant_id;

-- query: ingest_defaults
SELECT event_id, amount, price, tags, t3, kind, flag, derived FROM ingest WHERE tenant_id IN ('acme', 'gz', 'tsv') ORDER BY event_id;

-- query: ingest_tabseparated_readback
-- formats: TabSeparated,TSVWithNames,TSVRaw
SELECT event_id, note FROM ingest WHERE note IS NOT NULL ORDER BY event_id;

-- query: ingest_json_readback
-- formats: JSON,JSONEachRow,JSONCompactEachRow
SELECT event_id, note, tags FROM ingest WHERE tenant_id IN ('acme', 'csv', 'vals') ORDER BY event_id;

-- query: ingest_csv_readback
-- formats: CSV,CSVWithNames
SELECT event_id, note, amount FROM ingest WHERE tenant_id IN ('csv', 'tsv') ORDER BY event_id;

-- query: ingest_default_format_param
-- setting: default_format=JSONEachRow
SELECT count() AS c FROM ingest FORMAT JSONEachRow;

-- Raw JSON numbers as strings, booleans as 0/1, missing nullable.
INSERT INTO ingest FORMAT JSONEachRow
{"tenant_id":"loose","event_time":"2024-05-10 00:00:00","event_id":"l1","amount":"12","price":"3.5","flag":1}
{"tenant_id":"loose","event_time":"2024-05-10 00:00:01","event_id":"l2","price":1e2,"flag":false,"t3":"2024-05-10 00:00:01"};

-- query: ingest_loose_json
SELECT event_id, amount, price, flag, t3, note FROM ingest WHERE tenant_id = 'loose' ORDER BY event_id;

-- JSONCompactEachRow input.
INSERT INTO ingest (tenant_id, event_time, event_id, amount) FORMAT JSONCompactEachRow
["jc", "2024-05-11 00:00:00", "k1", 1]
["jc", "2024-05-11 00:00:01", "k2", 2];

-- query: ingest_json_compact_input
SELECT event_id, amount FROM ingest WHERE tenant_id = 'jc' ORDER BY event_id;
