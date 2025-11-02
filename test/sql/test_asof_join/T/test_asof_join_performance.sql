-- name: test_asof_join_performance @slow

DROP DATABASE IF EXISTS test_asof_join_performance;
CREATE DATABASE test_asof_join_performance;
use test_asof_join_performance;

CREATE TABLE sessions (
    visitor_id BIGINT NOT NULL COMMENT '访客ID',
    session_start DATETIME NOT NULL COMMENT '会话开始时间',
    session_id BIGINT NOT NULL COMMENT '会话ID',
    session_duration INT COMMENT '会话时长秒'
) ENGINE=OLAP
DUPLICATE KEY(visitor_id, session_start)
DISTRIBUTED BY HASH(visitor_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE visitors (
    visitor_id BIGINT NOT NULL COMMENT '访客ID',
    first_visit DATETIME NOT NULL COMMENT '首次访问时间',
    starting_session_id BIGINT NOT NULL COMMENT '起始会话ID',
    visitor_type VARCHAR(20) COMMENT '访客类型'
) ENGINE=OLAP
DUPLICATE KEY(visitor_id, first_visit)
DISTRIBUTED BY HASH(visitor_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO sessions (visitor_id, session_start, session_id, session_duration)
SELECT
    (number % 10000) as visitor_id,
    '2024-01-01 08:00:00' + INTERVAL number SECOND as session_start,
    number as session_id,
    30 + (number % 200000) as session_duration
FROM TABLE(generate_series(1, 1000000)) as t(number)
ORDER BY visitor_id, session_start;

INSERT INTO visitors (visitor_id, first_visit, starting_session_id, visitor_type)
SELECT
    (number % 10000) as visitor_id,
    '2024-01-01 07:30:00' + INTERVAL number SECOND as first_visit,
    number as starting_session_id,
    'aaaa'
FROM TABLE(generate_series(1, 50000000)) as t(number)
ORDER BY visitor_id, first_visit;

SELECT
    COUNT(*) as total_sessions,
    COUNT(v.first_visit) as valid_matches
FROM sessions s
ASOF JOIN visitors v
ON s.visitor_id = v.visitor_id AND v.first_visit <= s.session_start;

SELECT
    COUNT(*) as total_sessions,
    COUNT(v.first_visit) as valid_matches
FROM sessions s
ASOF LEFT JOIN visitors v
ON s.visitor_id = v.visitor_id AND v.first_visit <= s.session_start;


