-- name: test_asof_join_basic

DROP DATABASE IF EXISTS test_asof_join_basic;
CREATE DATABASE test_asof_join_basic;
use test_asof_join_basic;

-- Test tables for different temporal types
CREATE TABLE orders_datetime (
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_datetime (
  `product_id` int(11) NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE orders_date (
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `order_date` date NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_date (
  `product_id` int(11) NOT NULL,
  `price_date` date NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE orders_bigint (
  `order_id` int(11) NOT NULL,
  `user_id` int(11) NOT NULL,
  `order_timestamp` bigint NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_bigint (
  `product_id` int(11) NOT NULL,
  `price_timestamp` bigint NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Insert test data for datetime type
INSERT INTO orders_datetime VALUES
(1, 101, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 15:30:00', 200.00),
(3, 102, '2024-01-01 11:00:00', 150.00),
(4, 102, '2024-01-01 16:00:00', 300.00),
(5, 101, '2024-01-02 09:00:00', 250.00),
(6, 102, '2024-01-02 14:00:00', 180.00);

INSERT INTO prices_datetime VALUES
(101, '2024-01-01 08:00:00', 95.00),
(101, '2024-01-01 14:00:00', 105.00),
(101, '2024-01-02 08:00:00', 110.00),
(102, '2024-01-01 09:00:00', 90.00),
(102, '2024-01-01 13:00:00', 100.00),
(102, '2024-01-02 12:00:00', 115.00);

-- Insert test data for date type
INSERT INTO orders_date VALUES
(1, 201, '2024-01-01', 100.00),
(2, 201, '2024-01-02', 200.00),
(3, 202, '2024-01-01', 150.00),
(4, 202, '2024-01-03', 300.00);

INSERT INTO prices_date VALUES
(201, '2023-12-31', 95.00),
(201, '2024-01-01', 105.00),
(202, '2023-12-31', 90.00),
(202, '2024-01-02', 100.00);

-- Insert test data for bigint timestamp type
INSERT INTO orders_bigint VALUES
(1, 301, 1704067200, 100.00), -- 2024-01-01 10:00:00
(2, 301, 1704084600, 200.00), -- 2024-01-01 15:30:00
(3, 302, 1704070800, 150.00), -- 2024-01-01 11:00:00
(4, 302, 1704086400, 300.00); -- 2024-01-01 16:00:00

INSERT INTO prices_bigint VALUES
(301, 1704060000, 95.00), -- 2024-01-01 08:00:00
(301, 1704081600, 105.00), -- 2024-01-01 14:00:00
(302, 1704063600, 90.00), -- 2024-01-01 09:00:00
(302, 1704078000, 100.00); -- 2024-01-01 13:00:00

-- Test 1: ASOF INNER JOIN with datetime
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_datetime o
ASOF INNER JOIN prices_datetime p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test 2: ASOF LEFT OUTER JOIN with datetime
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_datetime o
ASOF LEFT JOIN prices_datetime p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test 3: ASOF INNER JOIN with date
SELECT o.order_id, o.user_id, o.order_date, p.price_date, p.price
FROM orders_date o
ASOF INNER JOIN prices_date p ON o.user_id = p.product_id AND o.order_date >= p.price_date
ORDER BY o.order_id;

-- Test 4: ASOF LEFT OUTER JOIN with date
SELECT o.order_id, o.user_id, o.order_date, p.price_date, p.price
FROM orders_date o
ASOF LEFT JOIN prices_date p ON o.user_id = p.product_id AND o.order_date >= p.price_date
ORDER BY o.order_id;

-- Test 5: ASOF INNER JOIN with bigint timestamp
SELECT o.order_id, o.user_id, o.order_timestamp, p.price_timestamp, p.price
FROM orders_bigint o
ASOF INNER JOIN prices_bigint p ON o.user_id = p.product_id AND o.order_timestamp >= p.price_timestamp
ORDER BY o.order_id;

-- Test 6: ASOF LEFT OUTER JOIN with bigint timestamp
SELECT o.order_id, o.user_id, o.order_timestamp, p.price_timestamp, p.price
FROM orders_bigint o
ASOF LEFT JOIN prices_bigint p ON o.user_id = p.product_id AND o.order_timestamp >= p.price_timestamp
ORDER BY o.order_id;
