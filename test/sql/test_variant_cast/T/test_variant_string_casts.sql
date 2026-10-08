-- name: test_variant_string_getters

SELECT variant_typeof(variant_query(v, '$.age')) AS actual_type,
       CAST(variant_query(v, '$.age') AS INT) AS direct_cast,
       get_variant_int(v, '$.age') AS get_int,
       CAST(get_variant_string(v, '$.age') AS INT) AS via_varchar,
       CAST(variant_query(v, '$.num') AS INT) AS numeric_control
FROM (SELECT CAST(named_struct('age', '35', 'num', 35) AS VARIANT) AS v) s;

SELECT a[1], a[2], a[3] IS NULL, a[4] IS NULL, array_length(a)
FROM (SELECT CAST(CAST(['35', '-2', 'bad', CAST(NULL AS VARCHAR)] AS VARIANT) AS ARRAY<INT>) AS a) s;

-- name: test_variant_string_numeric_casts

CREATE DATABASE test_variant_string_numeric_${uuid0};

USE test_variant_string_numeric_${uuid0};

SET sql_mode = '';

CREATE TABLE inputs (id INT NOT NULL, s VARCHAR(128))
DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO inputs VALUES
(1, '35'),
(2, '-128'),
(3, '127'),
(4, '128'),
(5, '9223372036854775807'),
(6, '9223372036854775808'),
(7, '3.14'),
(8, '2.5e1'),
(9, 'NaN'),
(10, 'Infinity'),
(11, 'bad'),
(12, NULL),
(13, ' 35 '),
(14, '170141183460469231731687303715884105727'),
(15, '170141183460469231731687303715884105728');

[ORDER] SELECT id,
       CAST(v AS TINYINT) <=> CAST(s AS TINYINT),
       CAST(v AS SMALLINT) <=> CAST(s AS SMALLINT),
       CAST(v AS INT) <=> CAST(s AS INT),
       CAST(v AS BIGINT) <=> CAST(s AS BIGINT),
       CAST(v AS LARGEINT) <=> CAST(s AS LARGEINT),
       CAST(v AS FLOAT) <=> CAST(s AS FLOAT),
       CAST(v AS DOUBLE) <=> CAST(s AS DOUBLE),
       get_variant_int(obj, '$.x') <=> CAST(s AS BIGINT),
       get_variant_double(obj, '$.x') <=> CAST(s AS DOUBLE)
FROM (SELECT id, s, CAST(s AS VARIANT) AS v,
             CAST(named_struct('x', s) AS VARIANT) AS obj FROM inputs) t
ORDER BY id;

DROP DATABASE test_variant_string_numeric_${uuid0};

-- name: test_variant_string_temporal_casts

CREATE DATABASE test_variant_string_temporal_${uuid0};

USE test_variant_string_temporal_${uuid0};

SET sql_mode = '';

CREATE TABLE inputs (id INT NOT NULL, d VARCHAR(64), dt VARCHAR(64), t VARCHAR(64))
DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO inputs VALUES
(1, '2024-02-29', '2025-02-02 03:04:05.123456', '01:02:03'),
(2, '20240229', '20250202030405', '25:00:00'),
(3, 'bad', 'bad', 'bad'),
(4, NULL, NULL, NULL),
(5, ' 2025-02-02 ', '2025-02-02', '839:00:00'),
(6, '2025-13-01', '2025-13-01 00:00:00', '01:60:00'),
(7, '', '', '01:02:03.5');

[ORDER] SELECT id,
       CAST(CAST(d AS VARIANT) AS DATE) <=> CAST(d AS DATE),
       CAST(CAST(dt AS VARIANT) AS DATETIME) <=> CAST(dt AS DATETIME),
       CAST(CAST(t AS VARIANT) AS TIME) <=> CAST(t AS TIME)
FROM inputs ORDER BY id;

SELECT CAST(CAST(CAST(d AS VARIANT) AS DATE) AS VARCHAR),
       CAST(CAST(CAST(dt AS VARIANT) AS DATETIME) AS VARCHAR),
       CAST(TIME_TO_SEC(CAST(CAST(t AS VARIANT) AS TIME)) AS BIGINT)
FROM inputs WHERE id = 1;

DROP DATABASE test_variant_string_temporal_${uuid0};
