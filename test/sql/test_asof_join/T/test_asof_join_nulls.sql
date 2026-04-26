-- name: test_asof_join_nulls @slow

DROP DATABASE IF EXISTS test_asof_join_nulls;
CREATE DATABASE test_asof_join_nulls;
USE test_asof_join_nulls;

-- Probe table: allow NULLs on equi and temporal columns
CREATE TABLE orders_nulls (
  `order_id` int,
  `user_id` int,           -- nullable equi key
  `order_time` datetime,   -- nullable temporal
  `amount` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

-- Build table: allow NULLs on equi and temporal columns
CREATE TABLE prices_nulls (
  `product_id` int,        -- nullable equi key
  `price_time` datetime,   -- nullable temporal
  `price` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Data layout notes:
--  user_id/product_id: 101, 102, NULL
--  *_time: some NULLs to test temporal-null behavior

INSERT INTO orders_nulls VALUES
-- Baseline non-null temporal & equi
(1, 101, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 12:00:00', 120.00),
(3, 102, '2024-01-02 10:00:00', 130.00),
-- Probe equi NULL
(4, NULL, '2024-01-01 11:00:00', 90.00),
-- Probe temporal NULL
(5, 101, NULL, 110.00),
(6, 102, NULL, 140.00);

INSERT INTO prices_nulls VALUES
-- Build rows for 101 with increasing times
(101, '2024-01-01 09:00:00', 9.50),
(101, '2024-01-01 11:30:00', 10.50),
(101, '2024-01-01 12:30:00', 10.80),
-- Build rows for 102 with increasing times
(102, '2024-01-02 08:00:00', 12.00),
(102, '2024-01-02 12:00:00', 12.50),
-- Build equi NULL
(NULL, '2024-01-01 08:00:00', 8.88),
-- Build temporal NULL
(101, NULL, 10.00),
(102, NULL, 12.34);

-- Case A: equi non-null, temporal non-null (baseline) - INNER
SELECT /* A1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;

-- Case A: baseline - LEFT
SELECT /* A2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;

-- Case B: probe equi NULL - INNER (should drop rows with NULL equi key)
SELECT /* B1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id = 4
ORDER BY o.order_id;

-- Case B: probe equi NULL - LEFT (should retain probe row, NULL build side)
SELECT /* B2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id = 4
ORDER BY o.order_id;

-- Case C: probe temporal NULL - INNER (ASOF predicate can't evaluate -> drop)
SELECT /* C1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (5,6)
ORDER BY o.order_id;

-- Case C: probe temporal NULL - LEFT (should retain probe row, NULL build side)
SELECT /* C2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (5,6)
ORDER BY o.order_id;

-- Case D: build temporal NULL - INNER (build rows with NULL price_time should be ignored)
SELECT /* D1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;

-- Case D: build temporal NULL - LEFT (same expectation, presence of NULL price_time rows doesn't affect)
SELECT /* D2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;

-- Case E: build equi NULL rows should not participate - LEFT shows no matches from those build rows
SELECT /* E1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,4,5)
ORDER BY o.order_id;

-- -----------------------------------------------------------------------------------
-- Additional null-handling tests across hash map types
-- -----------------------------------------------------------------------------------

-- 1) DirectMappingJoinHashMap (TINYINT)
CREATE TABLE dm_orders_nulls (
  order_id int,
  user_id tinyint,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
CREATE TABLE dm_prices_nulls (
  product_id tinyint,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO dm_orders_nulls VALUES
(1, 1,  '2024-01-01 10:00:00', 10.0),
(2, 1,  NULL,                  20.0),
(3, NULL,'2024-01-01 11:00:00', 30.0);
INSERT INTO dm_prices_nulls VALUES
(1, '2024-01-01 09:00:00', 9.0),
(1, NULL,                  9.5),
(NULL,'2024-01-01 08:00:00',8.8);
SELECT /* DM-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM dm_orders_nulls o ASOF INNER JOIN dm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
SELECT /* DM-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM dm_orders_nulls o ASOF LEFT JOIN dm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;

-- 2) RangeDirectMappingJoinHashMap (INT small range)
CREATE TABLE rdm_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
CREATE TABLE rdm_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
INSERT INTO rdm_orders_nulls VALUES
(1, 100, '2024-01-01 10:00:00', 10.0),
(2, 100, NULL,                  20.0),
(3, NULL,'2024-01-01 12:00:00', 30.0);
INSERT INTO rdm_prices_nulls VALUES
(100,'2024-01-01 09:00:00', 9.0),
(100,'2024-01-01 09:30:00', 9.5),
(NULL,'2024-01-01 08:00:00',8.8),
(100, NULL, 9.9);
SELECT /* RDM-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM rdm_orders_nulls o ASOF INNER JOIN rdm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
SELECT /* RDM-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM rdm_orders_nulls o ASOF LEFT JOIN rdm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;

-- 3) DenseRangeDirectMappingJoinHashMap (INT large interval, moderate rows)
CREATE TABLE dense_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
CREATE TABLE dense_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- build side ~500k rows to satisfy Dense
INSERT INTO dense_prices_nulls
SELECT 
  1600000000 + (generate_series - 1) * 10 + 10,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  9.0 + (generate_series % 100)
FROM TABLE(generate_series(1, 500000));
-- probe side small, with a few NULLs
INSERT INTO dense_orders_nulls VALUES
(1, 1600000010, '2024-01-01 10:00:00', 10.0),
(2, 1600001010, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
SELECT count(*)
FROM dense_orders_nulls o ASOF INNER JOIN [broadcast] dense_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time;
SELECT count(*)
FROM dense_orders_nulls o ASOF LEFT JOIN [broadcast] dense_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time;

-- 4) LinearChainedJoinHashMap
set enable_hash_join_range_direct_mapping_opt=false;
CREATE TABLE lc_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
CREATE TABLE lc_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- build side small so Dense memory test fails
INSERT INTO lc_prices_nulls
SELECT 
  1700000000 + (generate_series - 1) * 1000,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  20.0 + (generate_series % 10)
FROM TABLE(generate_series(1, 5));
INSERT INTO lc_orders_nulls VALUES
(1, 1700000000, '2024-01-01 10:00:00', 10.0),
(2, 1700050000, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
SELECT /* LC-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lc_orders_nulls o ASOF INNER JOIN lc_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
SELECT /* LC-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lc_orders_nulls o ASOF LEFT JOIN lc_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;

-- 5) LinearChainedAsofJoinHashMap (fallback): disable linear chained opt then ASOF fallback kicks in
CREATE TABLE lca_orders_nulls LIKE lc_orders_nulls;
CREATE TABLE lca_prices_nulls LIKE lc_prices_nulls;
INSERT INTO lca_prices_nulls VALUES (1800000000,'2024-01-01 08:00:00', 30.0);
INSERT INTO lca_orders_nulls VALUES (1,1800000000,'2024-01-01 10:00:00',10.0),(2,NULL,'2024-01-01 11:00:00',20.0),(3,1800000000,NULL,30.0);
set enable_hash_join_linear_chained_opt=false;
SELECT /* LCA-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lca_orders_nulls o ASOF LEFT JOIN lca_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
set enable_hash_join_linear_chained_opt=true;
set enable_hash_join_range_direct_mapping_opt=true;