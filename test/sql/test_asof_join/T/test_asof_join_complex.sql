-- name: test_asof_join_complex

DROP DATABASE IF EXISTS test_asof_join_complex;
CREATE DATABASE test_asof_join_complex;
use test_asof_join_complex;

CREATE TABLE transactions (
  `txn_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `account_id` varchar(20) NOT NULL,
  `txn_time` datetime NOT NULL,
  `amount` decimal(15,2) NOT NULL,
  `txn_type` varchar(20) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`txn_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE exchange_rates (
  `currency_pair` varchar(10) NOT NULL,
  `rate_time` datetime NOT NULL,
  `exchange_rate` decimal(10,6) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`currency_pair`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE user_profiles (
  `user_id` int(11) NOT NULL,
  `profile_time` datetime NOT NULL,
  `risk_level` varchar(20) NOT NULL,
  `credit_score` int(11) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`user_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE market_events (
  `event_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL,
  `currency` varchar(20) NOT NULL,
  `event_type` varchar(30) NOT NULL,
  `market_impact` decimal(5,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`event_id`)
PROPERTIES ("replication_num" = "1");

-- Insert test data
INSERT INTO transactions VALUES
(1, 101, 'USD', '2024-01-01 10:00:00', 1000.00, 'DEPOSIT'),
(2, 101, 'USD', '2024-01-01 15:30:00', 500.00, 'WITHDRAWAL'),
(3, 102, 'EUR', '2024-01-01 11:00:00', 2000.00, 'DEPOSIT'),
(4, 102, 'EUR', '2024-01-01 16:00:00', 800.00, 'WITHDRAWAL'),
(5, 101, 'USD', '2024-01-02 09:00:00', 1500.00, 'DEPOSIT'),
(6, 102, 'EUR', '2024-01-02 14:00:00', 1200.00, 'WITHDRAWAL');

INSERT INTO exchange_rates VALUES
('USD', '2024-01-01 08:00:00', 0.900000),
('USD', '2024-01-01 14:00:00', 0.950000),
('USD', '2024-01-02 08:00:00', 0.960000),
('EUR', '2024-01-01 08:00:00', 1.100000),
('EUR', '2024-01-01 13:00:00', 1.120000),
('EUR', '2024-01-02 08:00:00', 1.130000);

INSERT INTO user_profiles VALUES
(101, '2024-01-01 09:00:00', 'LOW', 760),
(101, '2024-01-01 11:30:00', 'MEDIUM', 805),
(101, '2024-01-02 08:00:00', 'HIGH', 860),
(102, '2024-01-01 10:00:00', 'LOW', 710),
(102, '2024-01-01 12:30:00', 'MEDIUM', 770),
(102, '2024-01-02 12:00:00', 'HIGH', 810);

INSERT INTO market_events VALUES
(1, '2024-01-01 08:00:00', 'USD', 'MARKET_OPEN', 0.50),
(2, '2024-01-01 12:00:00', 'USD', 'FED_ANNOUNCEMENT', 1.20),
(3, '2024-01-01 16:00:00', 'USD', 'MARKET_CLOSE', -0.30),
(4, '2024-01-02 08:30:00', 'EUR', 'MARKET_OPEN', 0.25);

-- Test 1: Multiple ASOF JOINs with different temporal conditions
SELECT *
FROM transactions t
ASOF LEFT JOIN exchange_rates er ON t.account_id = er.currency_pair AND t.txn_time >= er.rate_time
ASOF LEFT JOIN user_profiles up ON t.user_id = up.user_id AND t.txn_time >= up.profile_time
ASOF LEFT JOIN market_events me ON t.account_id = me.currency AND t.txn_time >= me.event_time
WHERE t.amount > 1000
ORDER BY t.txn_id;

-- Test 2: ASOF JOIN with time functions wrapping both sides of temporal predicates
SELECT 
    t.txn_id,
    t.user_id,
    DATE(t.txn_time) as txn_date,
    HOUR(t.txn_time) as txn_hour,
    er.exchange_rate,
    up.risk_level
FROM transactions t
ASOF INNER JOIN exchange_rates er
  ON t.account_id = er.currency_pair
 AND date_trunc('hour', t.txn_time) >= date_add(er.rate_time, INTERVAL 1 HOUR)
ASOF INNER JOIN user_profiles up
  ON t.user_id = up.user_id
 AND date_trunc('minute', t.txn_time) >= date_add(up.profile_time, INTERVAL 30 MINUTE)
WHERE DATE(t.txn_time) = '2024-01-01'
ORDER BY t.txn_id;

-- Test 3: ASOF JOIN with complex WHERE conditions
SELECT 
    t.txn_id,
    t.user_id,
    t.txn_time,
    t.amount,
    er.exchange_rate,
    up.risk_level,
    up.credit_score
FROM transactions t
ASOF INNER JOIN exchange_rates er ON t.account_id = er.currency_pair AND t.txn_time >= er.rate_time
ASOF INNER JOIN user_profiles up ON t.user_id = up.user_id AND t.txn_time >= up.profile_time
WHERE t.amount > 500 
  AND up.credit_score > 750
  AND er.exchange_rate > 0.85
ORDER BY t.txn_id;

-- Test 4: ASOF JOIN with aggregation
SELECT 
    t.user_id,
    COUNT(*) as txn_count,
    SUM(t.amount) as total_amount,
    AVG(er.exchange_rate) as avg_rate,
    MAX(up.credit_score) as max_credit_score
FROM transactions t
ASOF INNER JOIN exchange_rates er ON t.account_id = er.currency_pair AND t.txn_time >= er.rate_time
ASOF INNER JOIN user_profiles up ON t.user_id = up.user_id AND t.txn_time >= up.profile_time
GROUP BY t.user_id
ORDER BY t.user_id;

-- Test 5: ASOF JOIN with subquery
SELECT 
    t.txn_id,
    t.user_id,
    t.txn_time,
    t.amount,
    er.exchange_rate,
    up.risk_level
FROM transactions t
ASOF INNER JOIN exchange_rates er ON t.account_id = er.currency_pair AND t.txn_time >= er.rate_time
ASOF INNER JOIN user_profiles up ON t.user_id = up.user_id AND t.txn_time >= up.profile_time
WHERE t.user_id IN (
    SELECT DISTINCT user_id 
    FROM transactions 
    WHERE amount > 1000
)
ORDER BY t.txn_id;

-- Test 6: ASOF JOIN with different temporal operators
SELECT 
    t.txn_id,
    t.user_id,
    t.txn_time,
    t.amount,
    er.exchange_rate,
    up.risk_level
FROM transactions t
ASOF INNER JOIN exchange_rates er ON t.account_id = er.currency_pair AND t.txn_time > er.rate_time
ASOF INNER JOIN user_profiles up ON t.user_id = up.user_id AND t.txn_time <= up.profile_time
WHERE t.txn_type = 'DEPOSIT'
ORDER BY t.txn_id;

-- Test 7: ASOF JOIN with more function-wrapped temporal predicates (both sides)
SELECT 
    t.txn_id,
    t.user_id,
    t.txn_time,
    er.exchange_rate,
    me.event_type
FROM transactions t
ASOF INNER JOIN exchange_rates er
  ON t.account_id = er.currency_pair
 AND date_trunc('day', t.txn_time) >= date_add(er.rate_time, INTERVAL 0 DAY)
ASOF INNER JOIN market_events me
  ON t.account_id = me.currency
 AND date_add(t.txn_time, INTERVAL -30 MINUTE) >= date_trunc('minute', me.event_time)
WHERE t.txn_type = 'WITHDRAWAL'
ORDER BY t.txn_id;

-- Test 8: DATETIME(6) microsecond precision with non-equality temporal predicates
CREATE TABLE events_dt6 (
  `event_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `event_time` datetime NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`event_id`) PROPERTIES ("replication_num" = "1");

CREATE TABLE snapshots_dt6 (
  `user_id` int(11) NOT NULL,
  `snapshot_time` datetime NOT NULL,
  `snapshot_data` varchar(50) NOT NULL
) ENGINE=OLAP DISTRIBUTED BY HASH(`user_id`) PROPERTIES ("replication_num" = "1");

-- Multiple snapshots per equi key (user 701) at ms precision
INSERT INTO snapshots_dt6 VALUES
(701,'2024-01-01 10:00:00.100000','S1'),
(701,'2024-01-01 10:00:00.300000','S2'),
(701,'2024-01-01 10:00:00.900000','S3');

-- Multiple snapshots per equi key (user 702) at us precision
INSERT INTO snapshots_dt6 VALUES
(702,'2024-01-01 10:00:00.123456','T1'),
(702,'2024-01-01 10:00:00.223456','T2'),
(702,'2024-01-01 10:00:00.323456','T3');

-- Events around boundaries, ensuring multiple candidates exist per user
INSERT INTO events_dt6 VALUES
(1,701,'2024-01-01 10:00:00.350000'), -- between S2 and S3 (>= should pick S2)
(2,701,'2024-01-01 10:00:00.100001'), -- just after S1
(3,702,'2024-01-01 10:00:00.323455'), -- just before T3 (>= picks T2)
(4,702,'2024-01-01 10:00:00.500000'), -- after T3 (>= picks T3)
(5,702,'2024-01-01 10:00:00.123456'), -- equal to T1 boundary
(6,702,'2024-01-01 09:59:59.999999'); -- before first snapshot (no match for >=)

-- 8a: INNER with >= on DATETIME(6)
SELECT /* DT6-INNER-GE */ e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dt6 e
ASOF INNER JOIN snapshots_dt6 s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- 8b: LEFT with >= on DATETIME(6) (retains before-first row id=6)
SELECT /* DT6-LEFT-GE */ e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dt6 e
ASOF LEFT JOIN snapshots_dt6 s ON e.user_id = s.user_id AND e.event_time >= s.snapshot_time
ORDER BY e.event_id;

-- 8c: INNER with < on DATETIME(6) (choose nearest future snapshot if implementation supports)
SELECT /* DT6-INNER-LT */ e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dt6 e
ASOF INNER JOIN snapshots_dt6 s ON e.user_id = s.user_id AND e.event_time < s.snapshot_time
WHERE e.user_id IN (701,702)
ORDER BY e.event_id;

-- 8d: compare with date
SELECT /* DT6-INNER-GE */ e.event_id, e.user_id, e.event_time, s.snapshot_time, s.snapshot_data
FROM events_dt6 e
ASOF INNER JOIN snapshots_dt6 s ON e.user_id = s.user_id AND cast(e.event_time as date) >= cast(s.snapshot_time as date)
ORDER BY e.event_id;


-- Test 9: test asof join with other join conjunct
CREATE TABLE orders (
  `id` int(11) NULL COMMENT "订单ID",
  `product` varchar(50) NULL COMMENT "产品名称",
  `order_time` datetime NULL COMMENT "订单时间",
  `quantity` int(11) NULL COMMENT "订单数量",
  `max_price` decimal(10, 2) NULL COMMENT "最大价格"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE prices (
  `id` int(11) NULL COMMENT "价格记录ID",
  `product` varchar(50) NULL COMMENT "产品名称",
  `price_time` datetime NULL COMMENT "价格时间",
  `price` decimal(10, 2) NULL COMMENT "价格",
  `volume` int(11) NULL COMMENT "成交量"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

INSERT INTO orders VALUES
(1, 'A', '2024-01-01 10:00:00', 100, 100.00),
(2, 'A', '2024-01-01 11:30:00', 150, 60.00),
(3, 'A', '2024-01-01 14:00:00', 200, 50.00),
(4, 'A', '2024-01-01 16:00:00', 50, 120.00),
(5, 'B', '2024-01-01 09:30:00', 400, 25.00),
(6, 'B', '2024-01-01 12:00:00', 100, 30.00),
(7, 'B', '2024-01-01 15:30:00', 80, 40.00),
(8, 'C', '2024-01-01 13:00:00', 500, 20.00),
(9, 'C', '2024-01-01 17:00:00', 160, 50.00);

INSERT INTO prices VALUES
(1, 'A', '2024-01-01 08:00:00', 80.00, 1000),
(16, 'A', '2024-01-01 09:00:00', 60.00, 1000),
(2, 'A', '2024-01-01 09:30:00', 85.00, 1000),
(3, 'A', '2024-01-01 10:30:00', 53.33, 1500),
(4, 'A', '2024-01-01 12:00:00', 40.00, 1200),
(5, 'A', '2024-01-01 15:00:00', 120.00, 800),
(6, 'A', '2024-01-01 18:00:00', 90.00, 900),
(7, 'B', '2024-01-01 08:30:00', 20.00, 2000),
(8, 'B', '2024-01-01 10:00:00', 30.00, 1800),
(9, 'B', '2024-01-01 11:00:00', 80.00, 1500),
(17, 'B', '2024-01-01 13:00:00', 50.00, 2000),
(10, 'B', '2024-01-01 14:00:00', 100.00, 1400),
(11, 'B', '2024-01-01 16:30:00', 35.00, 1400),
(12, 'C', '2024-01-01 09:00:00', 16.00, 3000),
(13, 'C', '2024-01-01 12:30:00', 18.00, 2800),
(18, 'C', '2024-01-01 15:00:00', 30.00, 3000),
(14, 'C', '2024-01-01 16:00:00', 50.00, 1000),
(15, 'C', '2024-01-01 19:00:00', 45.00, 900);

SELECT
    o.id AS order_id,
    o.product,
    o.order_time,
    o.quantity,
    o.max_price,
    p.id AS price_id,
    p.product AS price_product,
    p.price_time,
    p.price,
    p.volume,
    o.quantity * p.price AS total_amount,
    TIMESTAMPDIFF(MINUTE, p.price_time, o.order_time) AS time_diff_min,
    CASE
        WHEN p.id IS NULL THEN 'NO_MATCH'
        ELSE 'MATCHED'
    END AS match_status
FROM orders o
ASOF LEFT JOIN prices p
    ON o.product = p.product
    AND o.order_time >= p.price_time
    AND CAST(o.quantity * p.price AS BIGINT) = CAST(8000 AS BIGINT)
ORDER BY o.product, o.order_time;

SELECT
    o.id AS order_id,
    o.product,
    o.order_time,
    o.quantity,
    o.max_price,
    p.id AS price_id,
    p.product AS price_product,
    p.price_time,
    p.price,
    p.volume,
    o.quantity * p.price AS total_amount,
    TIMESTAMPDIFF(MINUTE, p.price_time, o.order_time) AS time_diff_min,
    CASE
        WHEN p.id IS NULL THEN 'NO_MATCH'
        ELSE 'MATCHED'
    END AS match_status
FROM orders o
ASOF JOIN prices p
    ON o.product = p.product
    AND o.order_time >= p.price_time
    AND CAST(o.quantity * p.price AS BIGINT) = CAST(8000 AS BIGINT)
ORDER BY o.product, o.order_time;

