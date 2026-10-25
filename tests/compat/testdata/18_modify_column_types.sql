-- ALTER TABLE MODIFY COLUMN type changes over rows written before the ALTER
-- (stored values are converted like ClickHouse's MODIFY mutation).

CREATE TABLE mod_types (k UInt32, d DateTime, g Int64, c Float64, s String, w DateTime64(9)) ENGINE = MergeTree ORDER BY k;

INSERT INTO mod_types VALUES (1, '2024-01-01 13:00:00', 5, 2.7, '7', '2024-01-01 13:00:00.123456789'), (2, '2024-01-01 09:00:00', 6, -1.5, '42', '2024-01-02 00:00:00.5');

ALTER TABLE mod_types MODIFY COLUMN d Date;

ALTER TABLE mod_types MODIFY COLUMN g String;

ALTER TABLE mod_types MODIFY COLUMN c Int32;

ALTER TABLE mod_types MODIFY COLUMN s UInt8;

ALTER TABLE mod_types MODIFY COLUMN w DateTime64(3);

SELECT count() FROM mod_types WHERE d = '2024-01-01';

SELECT d, count() FROM mod_types GROUP BY d ORDER BY d;

SELECT k FROM mod_types WHERE g = '5';

SELECT k, d, g, c, s, w FROM mod_types ORDER BY k;

INSERT INTO mod_types VALUES (3, '2024-03-04', 'x', 9, 200, '2024-03-04 05:06:07.891');

SELECT k, d, g, c, s, w FROM mod_types ORDER BY k;

ALTER TABLE mod_types MODIFY COLUMN d DateTime;

SELECT k, d FROM mod_types ORDER BY k;
