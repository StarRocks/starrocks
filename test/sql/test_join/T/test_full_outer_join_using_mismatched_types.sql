-- name: test_full_outer_join_using_mismatched_types
-- Regression test: FULL OUTER JOIN ... USING(col) where the same-named
-- column has a different type on each side (e.g. DATETIME vs VARCHAR).
-- The planner must wrap the side whose type differs from the resolved
-- common type in an explicit CAST so that the synthesized COALESCE has
-- children matching its function signature.
DROP DATABASE IF EXISTS test_full_outer_join_using_mismatched_types_${uuid0};
CREATE DATABASE test_full_outer_join_using_mismatched_types_${uuid0};
USE test_full_outer_join_using_mismatched_types_${uuid0};

CREATE TABLE left_t (
    k INT,
    dt DATETIME,
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE right_t (
    k INT,
    dt VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");

INSERT INTO left_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (2, '2024-02-01 00:00:00', 'us-west', 'SFO'),
    (3, NULL, 'eu', 'LON');

INSERT INTO right_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (4, '2024-03-01 00:00:00', 'apac', 'TYO'),
    (5, NULL, NULL, 'BER');

-- The query must not error out: USING(dt, region, city) requires the
-- planner to coalesce a DATETIME with a VARCHAR for the `dt` column.
SELECT count(*) FROM left_t FULL OUTER JOIN right_t USING(dt, region, city);

-- Single-key cross-typed USING: dt is DATETIME on the left, VARCHAR on
-- the right.  Only the row whose datetime/string round-trip matches
-- joins; the rest become outer rows.
SELECT count(*) FROM left_t FULL OUTER JOIN right_t USING(dt);

-- Verify the synthesized COALESCE column has well-typed values for the
-- matched / outer rows.  Order by the deterministic key column.
SELECT
    coalesce(left_t.k, right_t.k) AS k,
    region,
    city
FROM left_t FULL OUTER JOIN right_t USING(region, city)
ORDER BY k, region, city;

DROP DATABASE test_full_outer_join_using_mismatched_types_${uuid0};
