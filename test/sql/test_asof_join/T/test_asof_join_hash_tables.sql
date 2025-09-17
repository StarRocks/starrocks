-- name: test_asof_join_hash_tables

DROP DATABASE IF EXISTS test_asof_join_hash_tables;
CREATE DATABASE test_asof_join_hash_tables;
use test_asof_join_hash_tables;

-- Test 1: DirectMappingJoinHashMap (TINYINT, SMALLINT, BOOLEAN)
CREATE TABLE orders_tinyint (
  `order_id` int(11) NOT NULL,
  `user_id` tinyint NOT NULL,  -- TINYINT triggers DirectMapping
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_tinyint (
  `product_id` tinyint NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Test 2: RangeDirectMappingJoinHashMap (INT with small range)
CREATE TABLE orders_range (
  `order_id` int(11) NOT NULL,
  `user_id` int NOT NULL,  -- Small range to trigger RangeDirectMapping
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_range (
  `product_id` int NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Test 3: DenseRangeDirectMappingJoinHashMap (INT with range that bypasses RangeDirectMapping)
-- Note: DENSE_RANGE_DIRECT_MAPPING is very difficult to trigger in practice
-- It requires value_interval > L2_CACHE_SIZE AND value_interval/4 + row_count*4 <= bucket_size*1.1*4
-- This is rarely used in real scenarios, so we'll use a simpler approach
CREATE TABLE orders_dense (
  `order_id` int(11) NOT NULL,
  `user_id` int NOT NULL,
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_dense (
  `product_id` int NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Test 4: LinearChainedJoinHashMap (INT with large range, small data)
CREATE TABLE orders_linear (
  `order_id` int(11) NOT NULL,
  `user_id` int NOT NULL,  -- Large range to trigger LinearChained
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_linear (
  `product_id` int NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Test 5: LinearChainedAsofJoinHashMap (fallback for ASOF JOIN)
CREATE TABLE orders_asof_fallback (
  `order_id` int(11) NOT NULL,
  `user_id` int NOT NULL,  -- Large range that doesn't fit other optimizations
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_asof_fallback (
  `product_id` int NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Test 6: SERIALIZED_VARCHAR (string keys)
CREATE TABLE orders_string (
  `order_id` int(11) NOT NULL,
  `user_code` varchar(20) NOT NULL,
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_string (
  `product_code` varchar(20) NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_code`)
PROPERTIES ("replication_num" = "1");

-- Test 7: Multiple keys (SERIALIZED_FIXED_SIZE_INT)
CREATE TABLE orders_multi_key (
  `order_id` int(11) NOT NULL,
  `user_id` int NOT NULL,
  `region_id` int NOT NULL,  -- Multiple keys trigger SERIALIZED_FIXED_SIZE
  `order_time` datetime NOT NULL,
  `amount` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`order_id`)
PROPERTIES ("replication_num" = "1");

CREATE TABLE prices_multi_key (
  `product_id` int NOT NULL,
  `region_id` int NOT NULL,
  `price_time` datetime NOT NULL,
  `price` decimal(10,2) NOT NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(`product_id`)
PROPERTIES ("replication_num" = "1");

-- Insert data for DirectMappingJoinHashMap (TINYINT range 1-10)
INSERT INTO orders_tinyint VALUES
(1, 1, '2024-01-01 10:00:00', 100.00),
(2, 2, '2024-01-01 15:30:00', 200.00),
(3, 3, '2024-01-01 11:00:00', 150.00),
(4, 4, '2024-01-01 16:00:00', 300.00),
(5, 5, '2024-01-02 09:00:00', 250.00),
(6, 6, '2024-01-02 14:00:00', 180.00);

INSERT INTO prices_tinyint VALUES
(1, '2024-01-01 08:00:00', 95.00),
(1, '2024-01-01 14:00:00', 105.00),
(2, '2024-01-01 09:00:00', 90.00),
(2, '2024-01-01 13:00:00', 100.00),
(3, '2024-01-01 10:00:00', 85.00),
(4, '2024-01-01 11:00:00', 110.00),
(5, '2024-01-01 12:00:00', 120.00),
(6, '2024-01-01 13:00:00', 130.00);

-- Insert data for RangeDirectMappingJoinHashMap (INT range 100-110, small range)
-- This should trigger RANGE_DIRECT_MAPPING because value_interval = 11 <= bucket_size
INSERT INTO orders_range VALUES
(1, 100, '2024-01-01 10:00:00', 100.00),
(2, 101, '2024-01-01 15:30:00', 200.00),
(3, 102, '2024-01-01 11:00:00', 150.00),
(4, 103, '2024-01-01 16:00:00', 300.00),
(5, 104, '2024-01-02 09:00:00', 250.00),
(6, 105, '2024-01-02 14:00:00', 180.00);

INSERT INTO prices_range VALUES
(100, '2024-01-01 08:00:00', 95.00),
(100, '2024-01-01 14:00:00', 105.00),
(101, '2024-01-01 09:00:00', 90.00),
(101, '2024-01-01 13:00:00', 100.00),
(102, '2024-01-01 10:00:00', 85.00),
(103, '2024-01-01 11:00:00', 110.00),
(104, '2024-01-01 12:00:00', 120.00),
(105, '2024-01-01 13:00:00', 130.00);

INSERT INTO prices_dense
SELECT 
    1500000000 + (generate_series - 1) * 10 + 10 as product_id,  -- range: 1500000010 to 1505000000
    '2024-01-01 08:00:00' + INTERVAL (generate_series - 1) SECOND as price_time,
    95.00 + (generate_series - 1) % 100 as price
FROM TABLE(generate_series(1, 5000000));

-- Probe-side can be smaller. Insert 500K rows into orders_dense covering same key lattice.
INSERT INTO orders_dense
SELECT 
    generate_series as order_id,
    1500000000 + generate_series * 10 as user_id,  -- range: 1500000010 to 1500000000+500000*10 = 1505000000
    '2024-01-01 10:00:00' + INTERVAL (generate_series - 1) SECOND as order_time,
    100.00 + (generate_series - 1) % 1000 as amount
FROM TABLE(generate_series(1, 500000));

-- Insert data for LinearChainedJoinHashMap (INT range 10000-20000, large range)
INSERT INTO orders_linear VALUES
(1, 10000, '2024-01-01 10:00:00', 100.00),
(2, 10001, '2024-01-01 15:30:00', 200.00),
(3, 10002, '2024-01-01 11:00:00', 150.00),
(4, 10003, '2024-01-01 16:00:00', 300.00),
(5, 10004, '2024-01-02 09:00:00', 250.00),
(6, 10005, '2024-01-02 14:00:00', 180.00);

INSERT INTO prices_linear VALUES
(10000, '2024-01-01 08:00:00', 95.00),
(10000, '2024-01-01 14:00:00', 105.00),
(10001, '2024-01-01 09:00:00', 90.00),
(10001, '2024-01-01 13:00:00', 100.00),
(10002, '2024-01-01 10:00:00', 85.00),
(10003, '2024-01-01 11:00:00', 110.00),
(10004, '2024-01-01 12:00:00', 120.00),
(10005, '2024-01-01 13:00:00', 130.00);

-- Insert data for LinearChainedAsofJoinHashMap (fallback case)
INSERT INTO orders_asof_fallback VALUES
(1, 100000, '2024-01-01 10:00:00', 100.00),
(2, 100001, '2024-01-01 15:30:00', 200.00),
(3, 100002, '2024-01-01 11:00:00', 150.00),
(4, 100003, '2024-01-01 16:00:00', 300.00);

INSERT INTO prices_asof_fallback VALUES
(100000, '2024-01-01 08:00:00', 95.00),
(100000, '2024-01-01 14:00:00', 105.00),
(100001, '2024-01-01 09:00:00', 90.00),
(100001, '2024-01-01 13:00:00', 100.00),
(100002, '2024-01-01 10:00:00', 85.00),
(100003, '2024-01-01 11:00:00', 110.00);

-- Insert data for SERIALIZED_VARCHAR (string keys)
INSERT INTO orders_string VALUES
(1, 'USER001', '2024-01-01 10:00:00', 100.00),
(2, 'USER002', '2024-01-01 15:30:00', 200.00),
(3, 'USER003', '2024-01-01 11:00:00', 150.00),
(4, 'USER004', '2024-01-01 16:00:00', 300.00);

INSERT INTO prices_string VALUES
('USER001', '2024-01-01 08:00:00', 95.00),
('USER001', '2024-01-01 14:00:00', 105.00),
('USER002', '2024-01-01 09:00:00', 90.00),
('USER002', '2024-01-01 13:00:00', 100.00),
('USER003', '2024-01-01 10:00:00', 85.00),
('USER004', '2024-01-01 11:00:00', 110.00);

-- Insert data for SERIALIZED_FIXED_SIZE_INT (multiple keys)
INSERT INTO orders_multi_key VALUES
(1, 100, 1, '2024-01-01 10:00:00', 100.00),
(2, 101, 1, '2024-01-01 15:30:00', 200.00),
(3, 102, 2, '2024-01-01 11:00:00', 150.00),
(4, 103, 2, '2024-01-01 16:00:00', 300.00);

INSERT INTO prices_multi_key VALUES
(100, 1, '2024-01-01 08:00:00', 95.00),
(100, 1, '2024-01-01 14:00:00', 105.00),
(101, 1, '2024-01-01 09:00:00', 90.00),
(101, 1, '2024-01-01 13:00:00', 100.00),
(102, 2, '2024-01-01 10:00:00', 85.00),
(103, 2, '2024-01-01 11:00:00', 110.00);


-- Test DirectMappingJoinHashMap
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_tinyint o
ASOF INNER JOIN prices_tinyint p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test RangeDirectMappingJoinHashMap
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_range o
ASOF INNER JOIN prices_range p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test DenseRangeDirectMappingJoinHashMap
-- This should trigger DENSE_RANGE_DIRECT_MAPPING due to large value_interval and sufficient row_count
SELECT count(*)
FROM orders_dense o
ASOF INNER JOIN [broadcast] prices_dense p ON o.user_id = p.product_id AND o.order_time >= p.price_time;

set enable_hash_join_range_direct_mapping_opt=false;
-- Test LinearChainedJoinHashMap
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_linear o
ASOF INNER JOIN prices_linear p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test LinearChainedAsofJoinHashMap (fallback)
set enable_hash_join_linear_chained_opt=false;
SELECT o.order_id, o.user_id, o.order_time, p.price_time, p.price
FROM orders_asof_fallback o
ASOF INNER JOIN prices_asof_fallback p ON o.user_id = p.product_id AND o.order_time >= p.price_time
ORDER BY o.order_id;
set enable_hash_join_range_direct_mapping_opt=true;
set enable_hash_join_linear_chained_opt=true;

-- Test SERIALIZED_VARCHAR
SELECT o.order_id, o.user_code, o.order_time, p.price_time, p.price
FROM orders_string o
ASOF INNER JOIN prices_string p ON o.user_code = p.product_code AND o.order_time >= p.price_time
ORDER BY o.order_id;

-- Test SERIALIZED_FIXED_SIZE_INT (multiple keys)
SELECT o.order_id, o.user_id, o.region_id, o.order_time, p.price_time, p.price
FROM orders_multi_key o
ASOF INNER JOIN prices_multi_key p ON o.user_id = p.product_id AND o.region_id = p.region_id AND o.order_time >= p.price_time
ORDER BY o.order_id;
