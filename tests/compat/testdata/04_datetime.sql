-- Date/time functions, formatting, timezones and boundary values.

CREATE TABLE ev (
    id UInt32,
    d Date,
    t DateTime,
    t3 DateTime64(3),
    tz DateTime('Asia/Tokyo'),
    nt Nullable(DateTime)
) ENGINE = MergeTree ORDER BY id;

INSERT INTO ev VALUES
    (1, '1970-01-01', '1970-01-01 00:00:00', '1970-01-01 00:00:00.000', '1970-01-01 09:00:00', NULL),
    (2, '2024-02-29', '2024-02-29 23:59:59', '2024-02-29 23:59:59.999', '2024-03-01 08:59:59', '2024-02-29 23:59:59'),
    (3, '2149-06-06', '2106-02-07 06:28:15', '2106-02-07 06:28:15.001', '2106-02-07 15:28:15', '2000-01-01 00:00:00'),
    (4, '2023-12-31', '2023-12-31 12:00:00', '2023-12-31 12:00:00.500', '2024-01-01 00:00:00', NULL),
    (5, '2024-06-15', '2024-06-15 10:30:45', '2024-06-15 10:30:45.123', '2024-06-15 19:30:45', '2024-06-15 10:30:45'),
    (6, '2000-01-03', '2000-01-03 00:00:01', '1999-12-31 23:59:59.999', '2000-01-03 09:00:01', NULL);

-- query
-- formats: TSVWithNamesAndTypes,JSONCompact,CSV,JSONEachRow,Values,Native
SELECT id, d, t, t3, tz, nt FROM ev ORDER BY id;

-- query
SELECT id, toYYYYMM(t), toYYYYMMDD(t), toYear(t), toMonth(t), toDayOfMonth(t), toHour(t), toMinute(t), toSecond(t) FROM ev ORDER BY id;

-- query
SELECT id, toStartOfDay(t), toStartOfHour(t), toStartOfMinute(t), toStartOfMonth(t), toStartOfWeek(t) FROM ev WHERE id > 1 ORDER BY id;

-- query: date_functions_saturate_at_1970
-- ClickHouse saturates Date results below 1970-01-01 (a Thursday) instead of
-- producing an unrepresentable Date.
SELECT toStartOfWeek(toDate('1970-01-01')), toMonday(toDate('1970-01-02')), toStartOfWeek(toDateTime('1970-01-01 05:00:00'));

-- query
SELECT id, toDate(t), toDate(t3), toDateTime(d), toUnixTimestamp(t), toDayOfWeek(t), toDayOfYear(t), toQuarter(t) FROM ev WHERE id != 3 ORDER BY id;

-- query: date_to_datetime_beyond_2106_wraps
-- Date goes to 2149 but DateTime only to 2106: ClickHouse wraps modulo 2^32.
SELECT toDateTime(toDate('2149-06-06')), toDateTime(d) FROM ev WHERE id = 3;

-- query
SELECT id, toStartOfWeek(d), toStartOfWeek(d, 1), toStartOfMonth(d), toMonday(d), toStartOfYear(d), toStartOfQuarter(d) FROM ev WHERE id > 1 ORDER BY id;

-- query
SELECT id, formatDateTime(t, '%Y-%m-%d %H:%M:%S'), formatDateTime(t, '%d/%m/%y %I:%M %p'), formatDateTime(t, '%F %T'), formatDateTime(t, '%j %u %w %e') FROM ev ORDER BY id;

-- query
SELECT id, formatDateTime(t3, '%Y-%m-%d %H:%i:%S'), formatDateTime(t, '%%'), formatDateTime(t, '%b %M %a') FROM ev ORDER BY id;

-- query
SELECT toDateTime('2024-01-15 12:00:00', 'America/New_York'), toDateTime('2024-07-15 12:00:00', 'America/New_York'), toDateTime('2024-01-15 12:00:00', 'Asia/Kolkata');

-- query
SELECT toTimeZone(toDateTime('2024-01-15 12:00:00', 'UTC'), 'Europe/Berlin'), toTypeName(toTimeZone(toDateTime('2024-01-15 12:00:00', 'UTC'), 'Europe/Berlin'));

-- query
SELECT toUnixTimestamp(toDateTime('2024-01-15 12:00:00', 'America/New_York')), toUnixTimestamp(toDateTime('2024-01-15 12:00:00'));

-- query
SELECT id, toString(tz), toHour(tz), toDate(tz), toStartOfDay(tz), toUnixTimestamp(tz) FROM ev WHERE id > 1 ORDER BY id;

-- query
SELECT toStartOfDay(toDateTime('2024-03-10 15:00:00', 'America/New_York')), toStartOfHour(toDateTime('2024-03-10 03:30:00', 'America/New_York'));

-- query
SELECT dateDiff('second', toDateTime('2024-01-01 00:00:00'), toDateTime('2024-01-01 00:01:30')), dateDiff('minute', toDateTime('2024-01-01 00:00:59'), toDateTime('2024-01-01 00:01:00')), dateDiff('hour', toDateTime('2024-01-01 00:00:00'), toDateTime('2024-01-02 01:00:00'));

-- query
SELECT dateDiff('day', toDate('2024-01-01'), toDate('2024-03-01')), dateDiff('month', toDate('2024-01-31'), toDate('2024-02-01')), dateDiff('year', toDate('2023-12-31'), toDate('2024-01-01')), dateDiff('week', toDate('2024-01-01'), toDate('2024-01-15'));

-- query
SELECT dateDiff('day', toDate('2024-03-01'), toDate('2024-01-01')), date_diff('second', toDateTime('2024-01-01 00:00:10'), toDateTime('2024-01-01 00:00:00'));

-- query
SELECT addSeconds(toDateTime('2024-01-01 00:00:00'), 90), addMinutes(toDateTime('2024-01-01 00:00:00'), -1), addHours(toDateTime('2024-01-01 00:00:00'), 25), addDays(toDate('2024-02-28'), 1), addDays(toDateTime('2024-02-28 10:00:00'), 2);

-- query
SELECT toDateTime('2024-01-01 00:00:00') + INTERVAL 1 HOUR, toDate('2024-01-31') + INTERVAL 1 MONTH, toDateTime('2024-01-01 00:00:00') - INTERVAL 30 SECOND, toDate('2024-01-01') - INTERVAL 1 DAY, toDate('2024-03-31') - INTERVAL 1 MONTH;

-- query
SELECT toTypeName(toDate('2024-01-01') + INTERVAL 1 DAY), toTypeName(toDateTime('2024-01-01 00:00:00') + INTERVAL 1 DAY), toTypeName(toDate('2024-01-01') + INTERVAL 1 HOUR);

-- query
SELECT toDate('2024-01-01') + 1, toDateTime('2024-01-01 00:00:00') + 1, toDate('2024-01-10') - toDate('2024-01-01');

-- query
SELECT toDateTime64('2024-01-01 00:00:00.123', 3), toDateTime64('2024-01-01 00:00:00.123456', 6), toDateTime64('2024-01-01 00:00:00', 3, 'Asia/Tokyo'), toDateTime64(1700000000, 3);

-- query
SELECT toDateTime64('2024-01-01 00:00:00.999', 3) + INTERVAL 1 SECOND, toStartOfSecond(toDateTime64('2024-01-01 00:00:05.678', 3)), toUnixTimestamp(toDateTime64('2024-01-01 00:00:05.678', 3));

-- query
SELECT toDateTime(1700000000), toDateTime(0), toDateTime(4294967295), toDate(19000), toDate(0);

-- query
SELECT toDateTime('2024-01-05T10:20:30'), toDate('20240105'), toDate('2024-01-05 10:20:30');

-- query: todate_single_digit_month_day
SELECT toDate('2024-1-5'), toDate('2024-01-5');

-- error: 41
SELECT toDateTime('2024-01-05 10:20');

-- query
SELECT parseDateTimeBestEffort('2024-01-05T10:20:30Z'), parseDateTimeBestEffort('2024-01-05 10:20:30+02:00'), parseDateTimeBestEffort('2024-01-05'), parseDateTimeBestEffort('1704450030');

-- query
SELECT parseDateTimeBestEffortOrNull('not a date'), parseDateTimeBestEffortOrNull('2024-01-05 01:02:03'), parseDateTime64BestEffort('2024-01-05T10:20:30.250Z', 3);

-- error
SELECT parseDateTimeBestEffort('garbage');

-- query
SELECT id FROM ev WHERE t >= '2024-01-01 00:00:00' AND t < '2024-07-01 00:00:00' ORDER BY id;

-- query
SELECT id FROM ev WHERE t3 BETWEEN '2024-02-29 23:59:59.999' AND '2024-06-15 10:30:45.123' ORDER BY id;

-- query
SELECT id FROM ev WHERE d = '2024-02-29' OR toDate(t) = toDate('2023-12-31') ORDER BY id;

-- query
SELECT id FROM ev WHERE nt IS NULL ORDER BY id;

-- query
SELECT min(t), max(t), min(t3), max(t3), min(d), max(d), max(nt), min(nt) FROM ev;

-- query
SELECT toYYYYMM(t) AS m, count() FROM ev GROUP BY m ORDER BY m;

-- query
SELECT toStartOfHour(t3) AS h, count(), max(t3) FROM ev GROUP BY h ORDER BY h;

-- query
SELECT t3, toTypeName(t3), t3 > toDateTime64('2024-01-01 00:00:00', 3), t3 = toDateTime('2024-06-15 10:30:45') FROM ev ORDER BY id;

-- query
-- param: from=2024-01-01 00:00:00
-- param: to=2024-12-31 23:59:59
SELECT id FROM ev WHERE t BETWEEN {from:DateTime} AND {to:DateTime} ORDER BY id;

-- query
-- param: at=2024-06-15 10:30:45.123
SELECT id FROM ev WHERE t3 = {at:DateTime64(3)} ORDER BY id;

-- query
-- param: d=2024-02-29
SELECT {d:Date}, toTypeName({d:Date}), id FROM ev WHERE d = {d:Date};

-- query
SELECT toDate32('1950-05-05'), toDate32('2200-01-01'), toTypeName(toDate32('2000-01-01'));

-- query
SELECT toStartOfFiveMinutes(toDateTime('2024-01-01 10:07:00')), toStartOfFifteenMinutes(toDateTime('2024-01-01 10:17:00')), toStartOfTenMinutes(toDateTime('2024-01-01 10:17:00'));

-- unsupported: INTERVAL outside date arithmetic is outside the DESIGN §3 list
SELECT toStartOfInterval(toDateTime('2024-01-01 10:17:00'), INTERVAL 10 MINUTE);

-- query
SELECT toDateTime('2024-01-01 00:00:00', 'UTC') = toDateTime('2024-01-01 09:00:00', 'Asia/Tokyo');

-- error: 38
SELECT toDate('05/01/2024');

-- error: 6
SELECT toDateTime('2024-01-05T10:20:30Z');
