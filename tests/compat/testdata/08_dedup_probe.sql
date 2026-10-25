-- Workload shape 4: dedup probe with a tuple-IN over an Array(Tuple) query
-- parameter plus a DateTime BETWEEN window, and other parameter types.

CREATE TABLE clicks (
    tenant_id String,
    event_time DateTime,
    event_id String,
    campaign_id UInt64,
    amount Int64
) ENGINE = MergeTree ORDER BY (tenant_id, event_time, event_id);

INSERT INTO clicks VALUES
    ('acme', '2024-05-01 10:00:00', 'e1', 1, 10),
    ('acme', '2024-05-01 10:00:01', 'e2', 1, -3),
    ('acme', '2024-05-01 12:00:00', 'e3', 2, 0),
    ('acme', '2024-05-03 00:00:00', 'e4', 2, 7),
    ('globex', '2024-05-01 10:00:00', 'e1', 9, 1),
    ('globex', '2024-05-01 11:00:00', 'g''q', 9, 2),
    ('initech', '2024-05-02 00:00:00', 'x', 3, 3);

-- query: dedup_probe
-- param: keys=[('acme','e1'),('acme','e3'),('globex','e1'),('globex','nope'),('zzz','e1')]
-- param: from=2024-05-01 00:00:00
-- param: to=2024-05-02 00:00:00
SELECT tenant_id, event_id FROM clicks
WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
  AND event_time BETWEEN {from:DateTime} AND {to:DateTime}
ORDER BY tenant_id, event_id;

-- query: dedup_probe_window_excludes
-- param: keys=[('acme','e4'),('initech','x')]
-- param: from=2024-05-01 00:00:00
-- param: to=2024-05-02 00:00:00
SELECT tenant_id, event_id FROM clicks
WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
  AND event_time BETWEEN {from:DateTime} AND {to:DateTime}
ORDER BY tenant_id, event_id;

-- query: dedup_probe_quote_in_param
-- param: keys=[('globex','g\'q')]
-- param: from=2024-01-01 00:00:00
-- param: to=2025-01-01 00:00:00
SELECT tenant_id, event_id, amount FROM clicks
WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))}
  AND event_time BETWEEN {from:DateTime} AND {to:DateTime};

-- query: dedup_probe_empty_array
-- param: keys=[]
SELECT count() FROM clicks WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))};

-- query: dedup_probe_count
-- param: keys=[('acme','e1'),('acme','e2')]
SELECT count() AS dupes FROM clicks WHERE (tenant_id, event_id) IN {keys:Array(Tuple(String, String))};

-- query: tuple_in_literal
SELECT event_id FROM clicks WHERE (tenant_id, event_id) IN (('acme', 'e2'), ('initech', 'x')) ORDER BY event_id;

-- query: tuple_in_subquery
SELECT tenant_id, event_id FROM clicks WHERE (tenant_id, event_id) IN (SELECT tenant_id, event_id FROM clicks WHERE amount > 2) ORDER BY tenant_id, event_id;

-- query: tuple_not_in
SELECT tenant_id, event_id FROM clicks WHERE (tenant_id, event_id) NOT IN (('acme', 'e1'), ('globex', 'e1')) ORDER BY tenant_id, event_id;

-- query: three_tuple_in
-- param: keys=[('acme','2024-05-01 10:00:00','e1'),('acme','2024-05-01 10:00:00','e2')]
SELECT event_id FROM clicks WHERE (tenant_id, event_time, event_id) IN {keys:Array(Tuple(String, DateTime, String))};

-- query: param_scalar_types
-- param: u=18446744073709551615
-- param: i=-9223372036854775808
-- param: f=0.1
-- param: b=true
-- param: s=héllo
SELECT {u:UInt64}, {i:Int64}, {f:Float64}, {b:Bool}, {s:String}, toTypeName({b:Bool});

-- query: param_nullable
-- param: n=\N
-- param: m=5
SELECT {n:Nullable(String)}, {m:Nullable(UInt8)}, isNull({n:Nullable(String)});

-- query: param_array_types
-- param: a=[1,2,3]
-- param: s=['x','y\'z']
-- param: e=[]
SELECT {a:Array(UInt32)}, {s:Array(String)}, {e:Array(String)}, length({a:Array(UInt32)}), has({s:Array(String)}, 'x');

-- query: param_map_tuple
-- param: m={'a':1,'b':2}
-- param: t=('x',5)
SELECT {m:Map(String, UInt8)}, {t:Tuple(String, UInt8)}, {m:Map(String, UInt8)}['b'], tupleElement({t:Tuple(String, UInt8)}, 2);

-- query: param_datetime64
-- param: t=2024-05-01 10:00:00.123
SELECT {t:DateTime64(3)}, toTypeName({t:DateTime64(3)}), toUnixTimestamp({t:DateTime64(3)});

-- query: param_in_where_campaign
-- param: c=[1,9]
SELECT event_id FROM clicks WHERE campaign_id IN {c:Array(UInt64)} ORDER BY tenant_id, event_id;

-- query: param_reused
-- param: t=acme
SELECT count() FROM clicks WHERE tenant_id = {t:String} OR tenant_id = concat({t:String}, '');

-- param: tbl=clicks
SELECT count() FROM {tbl:Identifier};

-- error: 456
SELECT {missing:String};

-- error
-- param: u=notanumber
SELECT {u:UInt64};

-- query: param_uint8_out_of_range_wraps
-- param: u=300
SELECT {u:UInt8};
