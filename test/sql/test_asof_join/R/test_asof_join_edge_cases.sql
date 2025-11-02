-- name: test_asof_join_edge_cases
DROP DATABASE IF EXISTS test_asof_join_edge_cases;
-- result:
-- !result
CREATE DATABASE test_asof_join_edge_cases;
-- result:
-- !result
use test_asof_join_edge_cases;
-- result:
-- !result
CREATE TABLE events (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE events_with_nulls (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_with_nulls (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events VALUES
(1, 101, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 101, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 102, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 102, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 103, '2024-01-01 12:00:00', 'EVENT_E');
-- result:
-- !result
INSERT INTO snapshots VALUES
(101, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(101, '2024-01-01 14:00:00', 'SNAPSHOT_2'),
(102, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(102, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(103, '2024-01-01 11:30:00', 'SNAPSHOT_5');
-- result:
-- !result
INSERT INTO snapshots VALUES (101, '2024-01-01 10:00:00', 'SNAPSHOT_1_10AM');
-- result:
-- !result
INSERT INTO events_with_nulls VALUES
(1, 201, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 201, NULL, 'EVENT_B'),
(3, 202, '2024-01-01 11:00:00', 'EVENT_C'),
(4, 202, '2024-01-01 16:00:00', 'EVENT_D'),
(5, 203, NULL, 'EVENT_E');
-- result:
-- !result
INSERT INTO snapshots_with_nulls VALUES
(201, '2024-01-01 08:00:00', 'SNAPSHOT_1'),
(201, NULL, 'SNAPSHOT_2'),
(202, '2024-01-01 09:00:00', 'SNAPSHOT_3'),
(202, '2024-01-01 13:00:00', 'SNAPSHOT_4'),
(203, '2024-01-01 11:30:00', 'SNAPSHOT_5');
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id AND e.event_time < s.snapshot_time
WHERE e.user_id = 101
ORDER BY e.event_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 14:00:00	SNAPSHOT_2
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF LEFT JOIN snapshots s ON e.user_id = s.user_id AND e.event_time < s.snapshot_time
WHERE e.user_id = 101
ORDER BY e.event_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 14:00:00	SNAPSHOT_2
2	101	2024-01-01 15:30:00	None	None
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 10:00:00	SNAPSHOT_1_10AM
2	101	2024-01-01 15:30:00	2024-01-01 14:00:00	SNAPSHOT_2
3	102	2024-01-01 11:00:00	2024-01-01 09:00:00	SNAPSHOT_3
4	102	2024-01-01 16:00:00	2024-01-01 13:00:00	SNAPSHOT_4
5	103	2024-01-01 12:00:00	2024-01-01 11:30:00	SNAPSHOT_5
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots s ON e.user_id = s.user_id + 1000 AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
-- !result
CREATE TABLE empty_snapshots (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN empty_snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF LEFT JOIN empty_snapshots s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
1	101	2024-01-01 10:00:00	None	None
2	101	2024-01-01 15:30:00	None	None
3	102	2024-01-01 11:00:00	None	None
4	102	2024-01-01 16:00:00	None	None
5	103	2024-01-01 12:00:00	None	None
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF INNER JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
WHERE e.event_time IS NULL
ORDER BY e.event_id;
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF LEFT JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
WHERE e.event_time IS NULL
ORDER BY e.event_id;
-- result:
2	201	None	None	None
5	203	None	None	None
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events e
ASOF INNER JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_with_nulls e
ASOF LEFT JOIN snapshots_with_nulls s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
1	201	2024-01-01 10:00:00	2024-01-01 08:00:00	SNAPSHOT_1
2	201	None	None	None
3	202	2024-01-01 11:00:00	2024-01-01 09:00:00	SNAPSHOT_3
4	202	2024-01-01 16:00:00	2024-01-01 13:00:00	SNAPSHOT_4
5	203	None	None	None
-- !result
CREATE TABLE events_large_gap (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_large_gap (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events_large_gap VALUES
(1, 301, '2024-01-01 10:00:00', 'EVENT_A'),
(2, 301, '2024-01-01 15:30:00', 'EVENT_B'),
(3, 302, '2024-01-01 11:00:00', 'EVENT_C');
-- result:
-- !result
INSERT INTO snapshots_large_gap VALUES
(301, '2023-12-01 08:00:00', 'SNAPSHOT_1'),
(301, '2023-12-15 14:00:00', 'SNAPSHOT_2'),
(302, '2023-11-01 09:00:00', 'SNAPSHOT_3');
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_large_gap e
ASOF INNER JOIN snapshots_large_gap s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;
-- result:
1	301	2024-01-01 10:00:00	2023-12-15 14:00:00	SNAPSHOT_2
2	301	2024-01-01 15:30:00	2023-12-15 14:00:00	SNAPSHOT_2
3	302	2024-01-01 11:00:00	2023-11-01 09:00:00	SNAPSHOT_3
-- !result
CREATE TABLE events_dups (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_dups (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events_dups VALUES (1,401,'2024-01-01 10:00:00');
-- result:
-- !result
INSERT INTO snapshots_dups VALUES
(401,'2024-01-01 10:00:00','SNAP_D1'),
(401,'2024-01-01 10:00:00','SNAP_D2');
-- result:
-- !result
SELECT e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dups e
ASOF INNER JOIN snapshots_dups s ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
ORDER BY e.event_id;
-- result:
1	401	2024-01-01 10:00:00	2024-01-01 10:00:00	SNAP_D1
-- !result
CREATE TABLE events_ops (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_ops (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events_ops VALUES (1,501,'2024-01-01 12:00:00');
-- result:
-- !result
INSERT INTO snapshots_ops VALUES (501,'2024-01-01 12:00:00');
-- result:
-- !result
SELECT /* OP-< */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time < s.snapshot_time;
-- result:
-- !result
SELECT /* OP-<= */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time <= s.snapshot_time;
-- result:
1	2024-01-01 12:00:00	2024-01-01 12:00:00
-- !result
SELECT /* OP-> */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time > s.snapshot_time;
-- result:
-- !result
SELECT /* OP->= */ e.event_id, e.event_time, s.snapshot_time
FROM events_ops e ASOF INNER JOIN snapshots_ops s
ON e.user_id=s.user_id AND e.event_time >= s.snapshot_time;
-- result:
1	2024-01-01 12:00:00	2024-01-01 12:00:00
-- !result
CREATE TABLE events_ooo (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_ooo (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events_ooo VALUES (1,551,'2024-01-01 10:30:00');
-- result:
-- !result
INSERT INTO snapshots_ooo VALUES (551,'2024-01-01 10:00:00','S10'),(551,'2024-01-01 09:00:00','S09');
-- result:
-- !result
SELECT e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_ooo e ASOF INNER JOIN snapshots_ooo s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
ORDER BY e.event_id;
-- result:
1	2024-01-01 10:30:00	2024-01-01 10:00:00	S10
-- !result
CREATE TABLE events_bounds (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_bounds (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO snapshots_bounds VALUES (601,'2024-01-01 09:00:00','SB09'),(601,'2024-01-01 18:00:00','SB18');
-- result:
-- !result
INSERT INTO events_bounds VALUES (1,601,'2024-01-01 07:00:00'),(2,601,'2024-01-01 20:00:00');
-- result:
-- !result
SELECT /* B-INNER-BEFORE */ e.event_id, e.event_time, s.snapshot_time
FROM events_bounds e ASOF INNER JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=1;
-- result:
-- !result
SELECT /* B-LEFT-BEFORE */ e.event_id, e.event_time, s.snapshot_time
FROM events_bounds e ASOF LEFT JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=1;
-- result:
1	2024-01-01 07:00:00	None
-- !result
SELECT /* B-INNER-AFTER */ e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_bounds e ASOF INNER JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=2;
-- result:
2	2024-01-01 20:00:00	2024-01-01 18:00:00	SB18
-- !result
SELECT /* B-LEFT-AFTER */ e.event_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_bounds e ASOF LEFT JOIN snapshots_bounds s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time
WHERE e.event_id=2;
-- result:
2	2024-01-01 20:00:00	2024-01-01 18:00:00	SB18
-- !result
CREATE TABLE events_equi_null (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots_equi_null (
  `user_id` int(11) NULL,
  `snapshot_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events_equi_null VALUES (1,NULL,'2024-01-01 12:00:00');
-- result:
-- !result
INSERT INTO snapshots_equi_null VALUES (NULL,'2024-01-01 11:00:00');
-- result:
-- !result
SELECT /* EN-INNER */ e.event_id, e.user_id, e.event_time, s.snapshot_time
FROM events_equi_null e ASOF INNER JOIN snapshots_equi_null s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time;
-- result:
-- !result
SELECT /* EN-LEFT */ e.event_id, e.user_id, e.event_time, s.snapshot_time
FROM events_equi_null e ASOF LEFT JOIN snapshots_equi_null s
ON e.user_id=s.user_id AND e.event_time>=s.snapshot_time;
-- result:
1	None	2024-01-01 12:00:00	None
-- !result