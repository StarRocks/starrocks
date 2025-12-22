-- name: test_numeric_default
drop database if exists test_numeric_comprehensive;
-- result:
-- !result
CREATE DATABASE test_numeric_comprehensive;
-- result:
-- !result
USE test_numeric_comprehensive;
-- result:
-- !result
CREATE TABLE users_basic (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO users_basic VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie');
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN age TINYINT DEFAULT '25';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN score SMALLINT DEFAULT '100';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN salary INT DEFAULT '50000';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN revenue BIGINT DEFAULT '1000000';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN rating FLOAT DEFAULT '4.5';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN percentage DOUBLE DEFAULT '95.5';
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	25	100	50000	1000000	4.5	95.5
2	bob	25	100	50000	1000000	4.5	95.5
3	charlie	25	100	50000	1000000	4.5	95.5
-- !result
SELECT
    id,
    CASE WHEN age = 25 THEN 'PASS' ELSE 'FAIL' END as test_age,
    CASE WHEN score = 100 THEN 'PASS' ELSE 'FAIL' END as test_score,
    CASE WHEN salary = 50000 THEN 'PASS' ELSE 'FAIL' END as test_salary,
    CASE WHEN revenue = 1000000 THEN 'PASS' ELSE 'FAIL' END as test_revenue,
    CASE WHEN abs(rating - 4.5) < 0.01 THEN 'PASS' ELSE 'FAIL' END as test_rating,
    CASE WHEN abs(percentage - 95.5) < 0.01 THEN 'PASS' ELSE 'FAIL' END as test_percentage
FROM users_basic
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS	PASS
-- !result
INSERT INTO users_basic VALUES (4, 'david', 30, 200, 60000, 2000000, 3.8, 88.9);
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	25	100	50000	1000000	4.5	95.5
2	bob	25	100	50000	1000000	4.5	95.5
3	charlie	25	100	50000	1000000	4.5	95.5
4	david	30	200	60000	2000000	3.8	88.9
-- !result
CREATE TABLE products_with_key (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
-- result:
-- !result
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
-- result:
-- !result
ALTER TABLE products_with_key ADD COLUMN price1 DOUBLE DEFAULT '99.99';
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT COUNT(*) FROM products_with_key;
-- result:
3
-- !result
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    price DOUBLE DEFAULT '0.0',
    discount FLOAT DEFAULT '0.0',
    amount BIGINT DEFAULT '0'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
-- result:
-- !result
SET partial_update_mode = 'column';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	laptop	1	0.0	0.0	0
2	phone	1	0.0	0.0	0
3	tablet	1	0.0	0.0	0
-- !result
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (4, 'monitor', 2);
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name, price) VALUES (5, 'keyboard', 299.99);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	laptop	1	0.0	0.0	0
2	phone	1	0.0	0.0	0
3	tablet	1	0.0	0.0	0
4	monitor	2	0.0	0.0	0
5	keyboard	1	299.99	0.0	0
-- !result
ALTER TABLE orders_column_mode ADD COLUMN tax_rate DOUBLE DEFAULT '0.08';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (7, 'headset', 3);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	laptop	1	0.0	0.0	0	0.08
2	phone	1	0.0	0.0	0	0.08
3	tablet	1	0.0	0.0	0	0.08
4	monitor	2	0.0	0.0	0	0.08
5	keyboard	1	299.99	0.0	0	0.08
6	mouse	1	0.0	0.0	0	0.08
7	headset	3	0.0	0.0	0	0.08
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    age TINYINT DEFAULT '18',
    score INT DEFAULT '0',
    balance BIGINT DEFAULT '1000'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice	18	0	1000
2	bob	18	0	1000
3	charlie	18	0	1000
-- !result
INSERT INTO users_pk_table (user_id, username, score) VALUES (1, 'alice_updated', 100);
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, age) VALUES (2, 'bob_updated', 25);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_updated	18	100	1000
2	bob_updated	25	0	1000
3	charlie	18	0	1000
-- !result
ALTER TABLE users_pk_table ADD COLUMN credit_limit BIGINT DEFAULT '5000';
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (1, 'alice_v2', 10000);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	18	100	1000	10000
2	bob_updated	25	0	1000	5000
3	charlie	18	0	1000	5000
4	david	18	0	1000	5000
-- !result
UPDATE users_pk_table SET age = DEFAULT, score = DEFAULT WHERE user_id = 3;
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	18	100	1000	10000
2	bob_updated	25	0	1000	5000
3	charlie	18	0	1000	5000
4	david	18	0	1000	5000
-- !result
CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
-- result:
-- !result
ALTER TABLE event_logs ADD COLUMN event_count INT DEFAULT '1';
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	1
2	event_2	1
-- !result
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	1
2	event_2	1
3	event_3	1
-- !result
CREATE TABLE edge_case_numerics (
    id INT NOT NULL,
    -- Test different numeric types with various defaults
    tiny_val TINYINT DEFAULT '127',
    small_val SMALLINT DEFAULT '32767',
    int_val INT DEFAULT '2147483647',
    big_val BIGINT DEFAULT '9223372036854775807',
    float_val FLOAT DEFAULT '3.14159',
    double_val DOUBLE DEFAULT '2.718281828',
    zero_val INT DEFAULT '0',
    negative_val INT DEFAULT '-100'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO edge_case_numerics (id) VALUES (1), (2), (3);
-- result:
-- !result
SELECT * FROM edge_case_numerics ORDER BY id;
-- result:
1	127	32767	2147483647	9223372036854775807	3.14159	2.718281828	0	-100
2	127	32767	2147483647	9223372036854775807	3.14159	2.718281828	0	-100
3	127	32767	2147483647	9223372036854775807	3.14159	2.718281828	0	-100
-- !result
SELECT 
    id,
    CASE WHEN tiny_val = 127 THEN 'PASS' ELSE 'FAIL' END as test_tiny,
    CASE WHEN small_val = 32767 THEN 'PASS' ELSE 'FAIL' END as test_small,
    CASE WHEN int_val = 2147483647 THEN 'PASS' ELSE 'FAIL' END as test_int,
    CASE WHEN big_val = 9223372036854775807 THEN 'PASS' ELSE 'FAIL' END as test_big,
    CASE WHEN abs(float_val - 3.14159) < 0.001 THEN 'PASS' ELSE 'FAIL' END as test_float,
    CASE WHEN abs(double_val - 2.718281828) < 0.000001 THEN 'PASS' ELSE 'FAIL' END as test_double,
    CASE WHEN zero_val = 0 THEN 'PASS' ELSE 'FAIL' END as test_zero,
    CASE WHEN negative_val = -100 THEN 'PASS' ELSE 'FAIL' END as test_negative
FROM edge_case_numerics
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
-- !result
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    total_quantity BIGINT SUM DEFAULT '0',
    total_revenue DOUBLE SUM DEFAULT '0.0',
    max_price DOUBLE MAX DEFAULT '0.0',
    min_price DOUBLE MIN DEFAULT '999999.99'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
-- result:
-- !result
ALTER TABLE sales_summary ADD COLUMN avg_discount DOUBLE REPLACE DEFAULT '0.05';
-- result:
-- !result
SELECT * FROM sales_summary ORDER BY product_id, region;
-- result:
1	North	0	0.0	0.0	999999.99	0.05
2	South	0	0.0	0.0	999999.99	0.05
-- !result
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    stock_quantity INT DEFAULT '100',
    reorder_point INT DEFAULT '20'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
-- result:
-- !result
ALTER TABLE inventory_items ADD COLUMN max_stock INT DEFAULT '1000';
-- result:
-- !result
SELECT * FROM inventory_items ORDER BY item_id;
-- result:
1	widget	100	20	1000
2	gadget	100	20	1000
-- !result