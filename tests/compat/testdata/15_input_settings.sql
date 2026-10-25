-- Input-format settings a loader may send (each in its own table so one
-- unsupported setting does not hide the others).

CREATE TABLE iso_in (id String, t DateTime, t3 DateTime64(3)) ENGINE = MergeTree ORDER BY id;

CREATE TABLE skip_in (id String, n UInt32) ENGINE = MergeTree ORDER BY id;

CREATE TABLE null_in (id String, n UInt32, s String) ENGINE = MergeTree ORDER BY id;

-- Unknown keys are skipped by default; NULL into a non-Nullable column
-- becomes the default (input_format_null_as_default=1).
INSERT INTO null_in FORMAT JSONEachRow
{"id":"a","n":null,"s":null,"extra":[1,2]}
{"id":"b","n":3};

-- query: null_as_default
SELECT * FROM null_in ORDER BY id;

INSERT INTO skip_in FORMAT TSV
a	1
b	2;

-- query: tsv_plain_insert
SELECT * FROM skip_in ORDER BY id;

-- setting: date_time_input_format=best_effort
INSERT INTO iso_in FORMAT JSONEachRow
{"id":"b1","t":"2024-05-01T10:00:03Z","t3":"2024-05-01T10:00:03.250+02:00"}
{"id":"b2","t":"2024-05-01 10:00:04","t3":"1714557604"};

-- query: best_effort_datetime_input
SELECT * FROM iso_in ORDER BY id;
