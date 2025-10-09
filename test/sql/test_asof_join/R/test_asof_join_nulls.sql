-- name: test_asof_join_nulls
DROP DATABASE IF EXISTS test_asof_join_nulls;
-- result:
-- !result
CREATE DATABASE test_asof_join_nulls;
-- result:
-- !result
USE test_asof_join_nulls;
-- result:
-- !result
CREATE TABLE orders_nulls (
  `order_id` int,
  `user_id` int,           -- nullable equi key
  `order_time` datetime,   -- nullable temporal
  `amount` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
CREATE TABLE prices_nulls (
  `product_id` int,        -- nullable equi key
  `price_time` datetime,   -- nullable temporal
  `price` decimal(10,2)
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
INSERT INTO orders_nulls VALUES
(1, 101, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 12:00:00', 120.00),
(3, 102, '2024-01-02 10:00:00', 130.00),
(4, NULL, '2024-01-01 11:00:00', 90.00),
(5, 101, NULL, 110.00),
(6, 102, NULL, 140.00);
-- result:
-- !result
INSERT INTO prices_nulls VALUES
(101, '2024-01-01 09:00:00', 9.50),
(101, '2024-01-01 11:30:00', 10.50),
(101, '2024-01-01 12:30:00', 10.80),
(102, '2024-01-02 08:00:00', 12.00),
(102, '2024-01-02 12:00:00', 12.50),
(NULL, '2024-01-01 08:00:00', 8.88),
(101, NULL, 10.00),
(102, NULL, 12.34);
-- result:
-- !result
SELECT /* A1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 09:00:00	9.50
2	101	2024-01-01 12:00:00	2024-01-01 11:30:00	10.50
3	102	2024-01-02 10:00:00	2024-01-02 08:00:00	12.00
-- !result
SELECT /* A2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 09:00:00	9.50
2	101	2024-01-01 12:00:00	2024-01-01 11:30:00	10.50
3	102	2024-01-02 10:00:00	2024-01-02 08:00:00	12.00
-- !result
SELECT /* B1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id = 4
ORDER BY o.order_id;
-- result:
-- !result
SELECT /* B2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id = 4
ORDER BY o.order_id;
-- result:
4	None	2024-01-01 11:00:00	None	None
-- !result
SELECT /* C1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (5,6)
ORDER BY o.order_id;
-- result:
-- !result
SELECT /* C2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (5,6)
ORDER BY o.order_id;
-- result:
5	101	None	None	None
6	102	None	None	None
-- !result
SELECT /* D1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF INNER JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 09:00:00	9.50
2	101	2024-01-01 12:00:00	2024-01-01 11:30:00	10.50
3	102	2024-01-02 10:00:00	2024-01-02 08:00:00	12.00
-- !result
SELECT /* D2 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,2,3)
ORDER BY o.order_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 09:00:00	9.50
2	101	2024-01-01 12:00:00	2024-01-01 11:30:00	10.50
3	102	2024-01-02 10:00:00	2024-01-02 08:00:00	12.00
-- !result
SELECT /* E1 */ o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_nulls o
ASOF LEFT JOIN prices_nulls p ON o.user_id = p.product_id AND o.order_time >= p.price_time
WHERE o.order_id IN (1,4,5)
ORDER BY o.order_id;
-- result:
1	101	2024-01-01 10:00:00	2024-01-01 09:00:00	9.50
4	None	2024-01-01 11:00:00	None	None
5	101	None	None	None
-- !result
CREATE TABLE dm_orders_nulls (
  order_id int,
  user_id tinyint,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
CREATE TABLE dm_prices_nulls (
  product_id tinyint,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
INSERT INTO dm_orders_nulls VALUES
(1, 1,  '2024-01-01 10:00:00', 10.0),
(2, 1,  NULL,                  20.0),
(3, NULL,'2024-01-01 11:00:00', 30.0);
-- result:
-- !result
INSERT INTO dm_prices_nulls VALUES
(1, '2024-01-01 09:00:00', 9.0),
(1, NULL,                  9.5),
(NULL,'2024-01-01 08:00:00',8.8);
-- result:
-- !result
SELECT /* DM-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM dm_orders_nulls o ASOF INNER JOIN dm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	1	2024-01-01 10:00:00	2024-01-01 09:00:00
-- !result
SELECT /* DM-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM dm_orders_nulls o ASOF LEFT JOIN dm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	1	2024-01-01 10:00:00	2024-01-01 09:00:00
2	1	None	None
3	None	2024-01-01 11:00:00	None
-- !result
CREATE TABLE rdm_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
CREATE TABLE rdm_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
INSERT INTO rdm_orders_nulls VALUES
(1, 100, '2024-01-01 10:00:00', 10.0),
(2, 100, NULL,                  20.0),
(3, NULL,'2024-01-01 12:00:00', 30.0);
-- result:
-- !result
INSERT INTO rdm_prices_nulls VALUES
(100,'2024-01-01 09:00:00', 9.0),
(100,'2024-01-01 09:30:00', 9.5),
(NULL,'2024-01-01 08:00:00',8.8),
(100, NULL, 9.9);
-- result:
-- !result
SELECT /* RDM-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM rdm_orders_nulls o ASOF INNER JOIN rdm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	100	2024-01-01 10:00:00	2024-01-01 09:30:00
-- !result
SELECT /* RDM-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM rdm_orders_nulls o ASOF LEFT JOIN rdm_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	100	2024-01-01 10:00:00	2024-01-01 09:30:00
2	100	None	None
3	None	2024-01-01 12:00:00	None
-- !result
CREATE TABLE dense_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
CREATE TABLE dense_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
INSERT INTO dense_prices_nulls
SELECT 
  1600000000 + (generate_series - 1) * 10 + 10,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  9.0 + (generate_series % 100)
FROM TABLE(generate_series(1, 500000));
-- result:
-- !result
INSERT INTO dense_orders_nulls VALUES
(1, 1600000010, '2024-01-01 10:00:00', 10.0),
(2, 1600001010, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
-- result:
-- !result
SELECT count(*)
FROM dense_orders_nulls o ASOF INNER JOIN [broadcast] dense_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time;
-- result:
1
-- !result
SELECT count(*)
FROM dense_orders_nulls o ASOF LEFT JOIN [broadcast] dense_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time;
-- result:
3
-- !result
set enable_hash_join_range_direct_mapping_opt=false;
-- result:
-- !result
CREATE TABLE lc_orders_nulls (
  order_id int,
  user_id int,
  order_time datetime,
  amount decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(order_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
CREATE TABLE lc_prices_nulls (
  product_id int,
  price_time datetime,
  price decimal(10,2)
) ENGINE=OLAP DISTRIBUTED BY HASH(product_id) PROPERTIES ("replication_num"="1");
-- result:
-- !result
INSERT INTO lc_prices_nulls
SELECT 
  1700000000 + (generate_series - 1) * 1000,
  date_add('2024-01-01 08:00:00', INTERVAL (generate_series - 1) SECOND),
  20.0 + (generate_series % 10)
FROM TABLE(generate_series(1, 5));
-- result:
-- !result
INSERT INTO lc_orders_nulls VALUES
(1, 1700000000, '2024-01-01 10:00:00', 10.0),
(2, 1700050000, NULL,                  20.0),
(3, NULL,       '2024-01-01 10:30:00', 30.0);
-- result:
-- !result
SELECT /* LC-INNER */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lc_orders_nulls o ASOF INNER JOIN lc_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	1700000000	2024-01-01 10:00:00	2024-01-01 08:00:00
-- !result
SELECT /* LC-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lc_orders_nulls o ASOF LEFT JOIN lc_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	1700000000	2024-01-01 10:00:00	2024-01-01 08:00:00
2	1700050000	None	None
3	None	2024-01-01 10:30:00	None
-- !result
CREATE TABLE lca_orders_nulls LIKE lc_orders_nulls;
-- result:
-- !result
CREATE TABLE lca_prices_nulls LIKE lc_prices_nulls;
-- result:
-- !result
INSERT INTO lca_prices_nulls VALUES (1800000000,'2024-01-01 08:00:00', 30.0);
-- result:
-- !result
INSERT INTO lca_orders_nulls VALUES (1,1800000000,'2024-01-01 10:00:00',10.0),(2,NULL,'2024-01-01 11:00:00',20.0),(3,1800000000,NULL,30.0);
-- result:
-- !result
set enable_hash_join_linear_chained_opt=false;
-- result:
-- !result
SELECT /* LCA-LEFT */ o.order_id, o.user_id, o.order_time, p.price_time
FROM lca_orders_nulls o ASOF LEFT JOIN lca_prices_nulls p
ON o.user_id=p.product_id AND o.order_time>=p.price_time ORDER BY o.order_id;
-- result:
1	1800000000	2024-01-01 10:00:00	2024-01-01 08:00:00
2	None	2024-01-01 11:00:00	None
3	1800000000	None	None
-- !result
set enable_hash_join_linear_chained_opt=true;
-- result:
-- !result
set enable_hash_join_range_direct_mapping_opt=true;
-- result:
-- !result