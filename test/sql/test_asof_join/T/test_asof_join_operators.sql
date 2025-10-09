-- name: test_asof_join_operators

DROP DATABASE IF EXISTS test_asof_join_operators;
CREATE DATABASE test_asof_join_operators;
use test_asof_join_operators;

CREATE TABLE events (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `event_type` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE user_changes (
  `user_id` int(11) NOT NULL,
  `change_time` datetime NOT NULL,
  `change_type` varchar(20) NOT NULL,
  `new_value` varchar(50) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

-- Insert test data
INSERT INTO events VALUES
(1, 101, '2024-01-01 10:00:00', 'LOGIN'),
(2, 101, '2024-01-01 15:30:00', 'PURCHASE'),
(3, 102, '2024-01-01 11:00:00', 'LOGIN'),
(4, 102, '2024-01-01 16:00:00', 'PURCHASE'),
(5, 101, '2024-01-02 09:00:00', 'LOGOUT'),
(6, 102, '2024-01-02 14:00:00', 'LOGOUT'),
(7, 101, '2024-01-01 12:00:00', 'EXACT_MATCH_1'),
(8, 102, '2024-01-01 12:00:00', 'EXACT_MATCH_2'),
(9, 103, '2024-01-01 12:00:00', 'EXACT_MATCH_3');

INSERT INTO user_changes VALUES
(101, '2024-01-01 08:00:00', 'STATUS', 'ACTIVE'),
(101, '2024-01-01 14:00:00', 'STATUS', 'VIP'),
(101, '2024-01-02 08:00:00', 'STATUS', 'PREMIUM'),
(102, '2024-01-01 09:00:00', 'STATUS', 'ACTIVE'),
(102, '2024-01-01 13:00:00', 'STATUS', 'VIP'),
(102, '2024-01-02 12:00:00', 'STATUS', 'PREMIUM'),
(101, '2024-01-01 12:00:00', 'STATUS', 'EXACT_STATUS_1'),
(102, '2024-01-01 12:00:00', 'STATUS', 'EXACT_STATUS_2'),
(103, '2024-01-01 12:00:00', 'STATUS', 'EXACT_STATUS_3');

-- Test 1: ASOF JOIN with >= operator (default behavior)
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF INNER JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time >= uc.change_time
ORDER BY e.event_id;

-- Test 2: ASOF JOIN with > operator
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF INNER JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time > uc.change_time
ORDER BY e.event_id;

-- Test 3: ASOF JOIN with <= operator
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF INNER JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time <= uc.change_time
ORDER BY e.event_id;

-- Test 4: ASOF JOIN with < operator
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF INNER JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time < uc.change_time
ORDER BY e.event_id;

-- Test 5: ASOF LEFT JOIN with >= operator
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF LEFT JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time >= uc.change_time
ORDER BY e.event_id;

-- Test 6: ASOF LEFT JOIN with > operator
SELECT e.event_id, e.user_id, e.event_time, uc.change_time, uc.change_type, uc.new_value
FROM events e
ASOF LEFT JOIN user_changes uc ON e.user_id = uc.user_id AND e.event_time > uc.change_time
ORDER BY e.event_id;

