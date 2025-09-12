-- name: test_asof_join
CREATE DATABASE test_asof_join;
-- result:
-- !result
use test_asof_join;
-- result:
-- !result
CREATE TABLE orders (
  `order_id` int(11) NOT NULL COMMENT "订单ID",
  `user_id` int(11) NOT NULL COMMENT "用户ID",
  `order_time` datetime NOT NULL COMMENT "订单时间",
  `amount` decimal(10,2) NOT NULL COMMENT "订单金额"
) ENGINE=OLAP
COMMENT "订单表"
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES (
  "replication_num" = "1"
);
-- result:
-- !result
CREATE TABLE user_status (
  `user_id` int(11) NOT NULL COMMENT "用户ID",
  `status_time` datetime NOT NULL COMMENT "状态变更时间",
  `status` varchar(20) NOT NULL COMMENT "用户状态",
  `credit_score` int(11) NOT NULL COMMENT "信用评分"
) ENGINE=OLAP
COMMENT "用户状态表"
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES (
  "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO orders VALUES
(1, 101, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 15:30:00', 200.00),
(3, 102, '2024-01-01 11:00:00', 150.00),
(4, 102, '2024-01-01 16:00:00', 300.00),
(5, 101, '2024-01-02 09:00:00', 250.00),
(6, 102, '2024-01-02 14:00:00', 180.00);
-- result:
-- !result
INSERT INTO user_status VALUES
(101, '2024-01-01 08:00:00', 'NORMAL', 750),
(101, '2024-01-01 14:00:00', 'VIP', 850),
(101, '2024-01-02 08:00:00', 'PREMIUM', 900),
(102, '2024-01-01 09:00:00', 'NORMAL', 700),
(102, '2024-01-01 13:00:00', 'VIP', 800),
(102, '2024-01-02 12:00:00', 'PREMIUM', 950);
-- result:
-- !result
function: assert_explain_contains('SELECT * FROM orders o ASOF LEFT JOIN user_status us ON o.user_id = us.user_id AND o.order_time >= us.status_time', 'ASOF LEFT OUTER JOIN')
-- result:
None
-- !result