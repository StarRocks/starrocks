-- name: test_full_outer_join_using_mismatched_types
DROP DATABASE IF EXISTS test_full_outer_join_using_mismatched_types_${uuid0};
-- result:
-- !result
CREATE DATABASE test_full_outer_join_using_mismatched_types_${uuid0};
-- result:
-- !result
USE test_full_outer_join_using_mismatched_types_${uuid0};
-- result:
-- !result
CREATE TABLE left_t (
    k INT,
    dt DATETIME,
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE right_t (
    k INT,
    dt VARCHAR(255),
    region VARCHAR(255),
    city VARCHAR(255)
) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO left_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (2, '2024-02-01 00:00:00', 'us-west', 'SFO'),
    (3, NULL, 'eu', 'LON');
-- result:
-- !result
INSERT INTO right_t VALUES
    (1, '2024-01-01 00:00:00', 'us-east', 'NYC'),
    (4, '2024-03-01 00:00:00', 'apac', 'TYO'),
    (5, NULL, NULL, 'BER');
-- result:
-- !result
SELECT count(*) FROM left_t FULL OUTER JOIN right_t USING(dt, region, city);
-- result:
5
-- !result
SELECT count(*) FROM left_t FULL OUTER JOIN right_t USING(dt);
-- result:
5
-- !result
SELECT
    coalesce(left_t.k, right_t.k) AS k,
    region,
    city
FROM left_t FULL OUTER JOIN right_t USING(region, city)
ORDER BY k, region, city;
-- result:
1	us-east	NYC
2	us-west	SFO
3	eu	LON
4	apac	TYO
5	None	BER
-- !result
DROP DATABASE test_full_outer_join_using_mismatched_types_${uuid0};
-- result:
-- !result
