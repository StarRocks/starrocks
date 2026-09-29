-- name: test_asof_join_invalid_temporal
DROP DATABASE IF EXISTS test_asof_join_invalid_temporal;
-- result:
-- !result
CREATE DATABASE test_asof_join_invalid_temporal;
-- result:
-- !result
use test_asof_join_invalid_temporal;
-- result:
-- !result
CREATE TABLE events (
  `id` int(11) NOT NULL,
  `k` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `ingest_time` datetime NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE snapshots (
  `k` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `publish_time` datetime NOT NULL,
  `payload` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`k`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE marks (
  `k` int(11) NOT NULL,
  `mark_time` datetime NOT NULL,
  `label` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`k`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO events VALUES
(1, 10, '2024-01-01 10:00:00', '2024-01-01 09:00:00'),
(2, 10, '2024-01-01 12:00:00', '2024-01-01 11:00:00'),
(3, 20, '2024-01-01 13:00:00', '2024-01-01 12:00:00');
-- result:
-- !result
INSERT INTO snapshots VALUES
(10, '2024-01-01 08:00:00', '2024-01-01 07:00:00', 'S1'),
(10, '2024-01-01 11:00:00', '2024-01-01 10:00:00', 'S2'),
(20, '2024-01-01 09:00:00', '2024-01-01 08:00:00', 'T1');
-- result:
-- !result
INSERT INTO marks VALUES
(10, '2024-01-01 07:00:00', 'M1'),
(10, '2024-01-01 11:30:00', 'M2'),
(20, '2024-01-01 12:30:00', 'M3');
-- result:
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= e.ingest_time;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 70 to line 1, column 88. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: `test_asof_join_invalid_temporal`.`e`.`event_time` >= `test_asof_join_invalid_temporal`.`e`.`ingest_time`.')
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and s.snapshot_time >= s.publish_time;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 70 to line 1, column 91. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: `test_asof_join_invalid_temporal`.`s`.`snapshot_time` >= `test_asof_join_invalid_temporal`.`s`.`publish_time`.')
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and e.event_time >= s.snapshot_time;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 142 to line 1, column 160. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: `test_asof_join_invalid_temporal`.`e`.`event_time` >= `test_asof_join_invalid_temporal`.`s`.`snapshot_time`.')
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= cast("2024-01-01 00:00:00" as datetime);
-- result:
E: (1064, 'Getting analyzing error from line 1, column 70 to line 1, column 124. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: `test_asof_join_invalid_temporal`.`e`.`event_time` >= CAST('2024-01-01 00:00:00' AS DATETIME).')
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= e.event_time;
-- result:
E: (1064, 'Getting analyzing error from line 1, column 70 to line 1, column 88. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: `test_asof_join_invalid_temporal`.`e`.`event_time` >= `test_asof_join_invalid_temporal`.`e`.`event_time`.')
-- !result
select e.id from events e asof left join snapshots s on e.k = s.k and date_add(e.event_time, interval 1 hour) >= date_add(s.snapshot_time, interval hour(e.event_time) hour);
-- result:
E: (1064, 'Getting analyzing error from line 1, column 70 to line 1, column 171. Detail message: ASOF JOIN temporal condition must compare a column from the left side of the join with a column from the right side, found: date_add(`test_asof_join_invalid_temporal`.`e`.`event_time`, INTERVAL 1 HOUR) >= date_add(`test_asof_join_invalid_temporal`.`s`.`snapshot_time`, INTERVAL hour(`test_asof_join_invalid_temporal`.`e`.`event_time`) HOUR).')
-- !result
select e.id, s.payload, m.label from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and e.event_time >= m.mark_time order by e.id;
-- result:
1	S1	M1
2	S2	M2
3	T1	M3
-- !result
select e.id, s.payload, m.label from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and s.snapshot_time >= m.mark_time order by e.id;
-- result:
1	S1	M1
2	S2	M1
3	T1	None
-- !result
