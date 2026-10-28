-- name: test_asof_join_invalid_temporal

DROP DATABASE IF EXISTS test_asof_join_invalid_temporal;
CREATE DATABASE test_asof_join_invalid_temporal;
use test_asof_join_invalid_temporal;

CREATE TABLE events (
  `id` int(11) NOT NULL,
  `k` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `ingest_time` datetime NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE snapshots (
  `k` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `publish_time` datetime NOT NULL,
  `payload` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`k`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE marks (
  `k` int(11) NOT NULL,
  `mark_time` datetime NOT NULL,
  `label` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`k`)
PROPERTIES ("replication_num" = "1");

INSERT INTO events VALUES
(1, 10, '2024-01-01 10:00:00', '2024-01-01 09:00:00'),
(2, 10, '2024-01-01 12:00:00', '2024-01-01 11:00:00'),
(3, 20, '2024-01-01 13:00:00', '2024-01-01 12:00:00');

INSERT INTO snapshots VALUES
(10, '2024-01-01 08:00:00', '2024-01-01 07:00:00', 'S1'),
(10, '2024-01-01 11:00:00', '2024-01-01 10:00:00', 'S2'),
(20, '2024-01-01 09:00:00', '2024-01-01 08:00:00', 'T1');

INSERT INTO marks VALUES
(10, '2024-01-01 07:00:00', 'M1'),
(10, '2024-01-01 11:30:00', 'M2'),
(20, '2024-01-01 12:30:00', 'M3');

-- The temporal condition drives the ASOF match: the BE reads one operand from the probe side and the
-- other from the build side. A condition that reads one side twice leaves the join without a
-- build-side temporal column, so it must be rejected instead of reaching the BE.

-- Both operands come from the left side.
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= e.ingest_time;

-- Both operands come from the right side.
select e.id from events e asof left join snapshots s on e.k = s.k and s.snapshot_time >= s.publish_time;

-- Chained ASOF joins where the second one compares against the first one's right relation instead of
-- against its own build side.
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and e.event_time >= s.snapshot_time;

-- An operand that reads neither side cannot stand in for one of them.
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= cast("2024-01-01 00:00:00" as datetime);

-- The same column compared with itself: the degenerate form of reading one side twice.
select e.id from events e asof left join snapshots s on e.k = s.k and e.event_time >= e.event_time;

-- A single operand that mixes both sides is rejected the same way.
select e.id from events e asof left join snapshots s on e.k = s.k and date_add(e.event_time, interval 1 hour) >= date_add(s.snapshot_time, interval hour(e.event_time) hour);

-- Chained ASOF joins that do relate the two sides at every step keep working.
select e.id, s.payload, m.label from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and e.event_time >= m.mark_time order by e.id;

-- The left side of the second join spans two relations; a condition between them is still valid.
select e.id, s.payload, m.label from events e asof left join snapshots s on e.k = s.k and e.event_time >= s.snapshot_time asof left join marks m on e.k = m.k and s.snapshot_time >= m.mark_time order by e.id;
