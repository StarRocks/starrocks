-- name: test_asof_join_filter_above
CREATE DATABASE db_${uuid0};
-- result:
-- !result
USE db_${uuid0};
-- result:
-- !result
CREATE TABLE orders (
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `order_time` datetime NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE user_status (
  `user_id` int(11) NOT NULL,
  `status_time` datetime NOT NULL,
  `status` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE status_label (
  `status` varchar(20) NOT NULL,
  `label` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`status`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO orders VALUES
(1, 101, '2024-01-01 10:00:00'),
(2, 101, '2024-01-01 15:30:00'),
(3, 102, '2024-01-01 11:00:00'),
(4, 102, '2024-01-01 16:00:00'),
(5, 101, '2024-01-02 09:00:00'),
(6, 102, '2024-01-02 14:00:00');
-- result:
-- !result
INSERT INTO user_status VALUES
(101, '2024-01-01 08:00:00', 'NORMAL'),
(101, '2024-01-01 14:00:00', 'VIP'),
(101, '2024-01-02 08:00:00', 'PREMIUM'),
(102, '2024-01-01 09:00:00', 'NORMAL'),
(102, '2024-01-01 13:00:00', 'VIP'),
(102, '2024-01-02 12:00:00', 'PREMIUM');
-- result:
-- !result
INSERT INTO status_label VALUES ('VIP', 'gold');
-- result:
-- !result
SELECT o.order_id, us.status_time, us.status FROM orders o ASOF INNER JOIN user_status us ON o.user_id = us.user_id AND o.order_time >= us.status_time WHERE us.status = 'NORMAL' ORDER BY o.order_id;
-- result:
1	2024-01-01 08:00:00	NORMAL
3	2024-01-01 09:00:00	NORMAL
-- !result
SELECT o.order_id, us.status_time, us.status FROM orders o ASOF LEFT JOIN user_status us ON o.user_id = us.user_id AND o.order_time >= us.status_time WHERE us.status = 'NORMAL' ORDER BY o.order_id;
-- result:
1	2024-01-01 08:00:00	NORMAL
3	2024-01-01 09:00:00	NORMAL
-- !result
SELECT o.order_id, r.status_time, r.label FROM orders o ASOF INNER JOIN (SELECT us.user_id, us.status_time, sl.label FROM user_status us LEFT JOIN status_label sl ON us.status = sl.status) r ON o.user_id = r.user_id AND o.order_time >= r.status_time JOIN status_label x ON r.label = x.label ORDER BY o.order_id;
-- result:
2	2024-01-01 14:00:00	gold
4	2024-01-01 13:00:00	gold
-- !result
SELECT o.order_id, us.status FROM orders o ASOF INNER JOIN user_status us ON o.user_id = us.user_id AND o.order_time >= us.status_time WHERE o.user_id = 101 ORDER BY o.order_id;
-- result:
1	NORMAL
2	VIP
5	PREMIUM
-- !result
DROP DATABASE db_${uuid0};
-- result:
-- !result
