-- name: test_asof_join_edge_cases

DROP DATABASE IF EXISTS test_asof_join_edge_cases;
CREATE DATABASE test_asof_join_edge_cases;
use test_asof_join_edge_cases;

CREATE TABLE events (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE events_with_nulls (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE snapshots_with_nulls (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

-- Insert test data for normal cases
INSERT INTO events VALUES
(1, 101, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 101, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 102, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 102, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 103, '2024-01-01 12:00:00', 'EVENT_E');

INSERT INTO snapshots VALUES
(101, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(101, '2024-01-01 14:00:00', 'SNAPSHOT_2'),
(102, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(102, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(103, '2024-01-01 11:30:00', 'SNAPSHOT_5');

-- Ensure exact temporal match exists for user 101 at 10:00:00 (for Test 3)
INSERT INTO snapshots VALUES (101, '2024-01-01 10:00:00', 'SNAPSHOT_1_10AM');

-- Insert test data with NULL temporal values
INSERT INTO events_with_nulls VALUES
(1, 201, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 201, NULL, 'EVENT_B'),
(3, 202, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 202, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 203, NULL, 'EVENT_E');

INSERT INTO snapshots_with_nulls VALUES
(201, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(201, NULL, 'SNAPSHOT_2'),
(202, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(202, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(203, '2024-01-01 11:30:00', 'SNAPSHOT_5');

-- Test 1: INNER with e.event_time < s.snapshot_time (returns only events earlier than first future snapshot)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id AND e.event_time < s.snapshot_time
WHERE e.user_id = 101
ORDER BY e.event_id;

-- Test 2: LEFT with e.event_time < s.snapshot_time (keeps probe rows; unmatched get NULL build)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF LEFT JOIN snapshots s ON e.user_id = s.user_id AND e.event_time < s.snapshot_time
WHERE e.user_id = 101
ORDER BY e.event_id;


-- Test 4: Multiple temporal matches (should return closest match)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 5: No equi-join matches (should return no rows)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id + 1000 AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 6: Empty build table (should return no rows for INNER JOIN)
CREATE TABLE empty_snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN empty_snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 7: Empty build table (should return probe rows with NULL for LEFT JOIN)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF LEFT JOIN empty_snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 8a: Probe temporal NULL (INNER) -> rows with e.event_time IS NULL are dropped
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF INNER JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
WHERE e.event_time IS NULL
ORDER BY e.event_id;

-- Test 8b: Probe temporal NULL (LEFT) -> retain probe rows, build side NULL
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF LEFT JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
WHERE e.event_time IS NULL
ORDER BY e.event_id;

-- Test 9: NULL temporal values in build table (build NULL snapshot_time should be ignored)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 10: Both tables have NULL temporal values (LEFT retains probe side)
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF LEFT JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 11: Large temporal gaps
CREATE TABLE events_large_gap (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE snapshots_large_gap (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

INSERT INTO events_large_gap VALUES
(1, 301, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 301, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 302, '2024-01-01 11:00:00', 'EVENT_C');

INSERT INTO snapshots_large_gap VALUES
(301, '2023-12-01 08:00:00', 'SNAPSHOT_1'),
(301, '2023-12-15 14:00:00', 'SNAPSHOT_2'),
(302, '2023-11-01 09:00:00', 'SNAPSHOT_3');

SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_large_gap e
ASOF INNER JOIN snapshots_large_gap s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- Test 12: Duplicate snapshot_time for the same user (tie-breaking on equal times)
CREATE TABLE events_dups (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_dups (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_dups VALUES (1,401,'2024-01-01 10:00:00');
INSERT INTO snapshots_dups VALUES
(401,'2024-01-01 10:00:00','SNAP_D1'),
(401,'2024-01-01 10:00:00','SNAP_D2');
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dups e
ASOF INNER JOIN snapshots_dups s ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
ORDER BY e.event_id;

-- Test 13: Operator edge behavior (<, <=, >, >=) at equality boundary
CREATE TABLE events_ops (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_ops (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_ops VALUES (1,501,'2024-01-01 12:00:00');
INSERT INTO snapshots_ops VALUES (501,'2024-01-01 12:00:00');
-- < should NOT match on equality
SELECT /* OP-< */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time < s.snapshot_time;
-- <= should match on equality
SELECT /* OP-<= */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time <= s.snapshot_time;
-- > should NOT match on equality
SELECT /* OP-> */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time > s.snapshot_time;
-- >= should match on equality
SELECT /* OP->= */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time >= s.snapshot_time;

-- Test 14: Out-of-order build insertion should still select by time correctly
CREATE TABLE events_ooo (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_ooo (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_ooo VALUES (1,551,'2024-01-01 10:30:00');
-- Insert later time first, then earlier time
INSERT INTO snapshots_ooo VALUES (551,'2024-01-01 10:00:00','S10'),(551,'2024-01-01 09:00:00','S09');
SELECT e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_ooo e ASOF INNER JOIN snapshots_ooo s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
ORDER BY e.event_id;

-- Test 15: Before-first and after-last boundaries
CREATE TABLE events_bounds (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_bounds (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO snapshots_bounds VALUES (601,'2024-01-01 09:00:00','SB09'),(601,'2024-01-01 18:00:00','SB18');
INSERT INTO events_bounds VALUES (1,601,'2024-01-01 07:00:00'),(2,601,'2024-01-01 20:00:00');
-- INNER before-first -> no rows
SELECT /* B-INNER-BEFORE */ e.event_id, e.event_time, s.snapshot_time
FROM events_bounds e ASOF INNER JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=1;
-- LEFT before-first -> keep probe, NULL build
SELECT /* B-LEFT-BEFORE */ e.event_id, e.event_time, s.snapshot_time
FROM events_bounds e ASOF LEFT JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=1;
-- INNER after-last -> matches last snapshot
SELECT /* B-INNER-AFTER */ e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_bounds e ASOF INNER JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=2;
-- LEFT after-last -> also matches last snapshot
SELECT /* B-LEFT-AFTER */ e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_bounds e ASOF LEFT JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=2;

-- Test 16: Equi NULL with non-null temporal
CREATE TABLE events_equi_null (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
CREATE TABLE snapshots_equi_null (
  `user_id` int(11) NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
INSERT INTO events_equi_null VALUES (1,NULL,'2024-01-01 12:00:00');
INSERT INTO snapshots_equi_null VALUES (NULL,'2024-01-01 11:00:00');
-- INNER should drop, LEFT should retain probe with NULL build
SELECT /* EN-INNER */ e.event_id, e.user_id, e.event_time, s.snapshot_time
FROM events_equi_null e ASOF INNER JOIN snapshots_equi_null s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time;
SELECT /* EN-LEFT */ e.event_id, e.user_id, e.event_time, s.snapshot_time
FROM events_equi_null e ASOF LEFT JOIN snapshots_equi_null s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time;