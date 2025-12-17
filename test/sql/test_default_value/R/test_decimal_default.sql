-- name: test_decimal_default
drop database if exists test_decimal_comprehensive;
-- result:
-- !result
CREATE DATABASE test_decimal_comprehensive;
-- result:
-- !result
USE test_decimal_comprehensive;
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
ALTER TABLE users_basic ADD COLUMN balance DECIMAL(10, 2) DEFAULT '1000.50';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN interest_rate DECIMAL(5, 4) DEFAULT '0.0525';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN score DECIMAL(8, 3) DEFAULT '85.125';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN percentage DECIMAL(5, 2) DEFAULT '99.99';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN big_balance DECIMAL(50, 10) DEFAULT '123456789012345678901234567890.1234567890';
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
2	bob	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
3	charlie	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
-- !result
SELECT 
    id,
    CASE WHEN balance = 1000.50 THEN 'PASS' ELSE 'FAIL' END as test_balance,
    CASE WHEN interest_rate = 0.0525 THEN 'PASS' ELSE 'FAIL' END as test_rate,
    CASE WHEN score = 85.125 THEN 'PASS' ELSE 'FAIL' END as test_score,
    CASE WHEN percentage = 99.99 THEN 'PASS' ELSE 'FAIL' END as test_percentage,
    CASE WHEN big_balance = 123456789012345678901234567890.1234567890 THEN 'PASS' ELSE 'FAIL' END as test_dec256
FROM users_basic 
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS
-- !result
INSERT INTO users_basic VALUES (4, 'david', 2500.75, 0.0625, 92.5, 87.65, 999999999999999999999999999999.9999999999);
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
2	bob	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
3	charlie	1000.50	0.0525	85.125	99.99	123456789012345678901234567890.1234567890
4	david	2500.75	0.0625	92.500	87.65	999999999999999999999999999999.9999999999
-- !result
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    unit_price DECIMAL(10, 2) DEFAULT '0.00',
    tax_rate DECIMAL(5, 4) DEFAULT '0.0800',
    discount DECIMAL(5, 2) DEFAULT '0.00',
    total_amount DECIMAL(12, 2) DEFAULT '0.00'
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
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	0.00	0.0800	0.00	0.00
2	bob	0.00	0.0800	0.00	0.00
3	charlie	0.00	0.0800	0.00	0.00
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, unit_price) VALUES (4, 'david', 299.99);
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, tax_rate) VALUES (5, 'eve', 0.1000);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	0.00	0.0800	0.00	0.00
2	bob	0.00	0.0800	0.00	0.00
3	charlie	0.00	0.0800	0.00	0.00
4	david	299.99	0.0800	0.00	0.00
5	eve	0.00	0.1000	0.00	0.00
-- !result
ALTER TABLE orders_column_mode ADD COLUMN shipping_fee DECIMAL(8, 2) DEFAULT '15.50';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, unit_price) VALUES (7, 'grace', 499.99);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	0.00	0.0800	0.00	0.00	15.50
2	bob	0.00	0.0800	0.00	0.00	15.50
3	charlie	0.00	0.0800	0.00	0.00	15.50
4	david	299.99	0.0800	0.00	0.00	15.50
5	eve	0.00	0.1000	0.00	0.00	15.50
6	frank	0.00	0.0800	0.00	0.00	15.50
7	grace	499.99	0.0800	0.00	0.00	15.50
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    account_balance DECIMAL(12, 2) DEFAULT '1000.00',
    credit_limit DECIMAL(10, 2) DEFAULT '5000.00',
    interest_rate DECIMAL(5, 4) DEFAULT '0.0350'
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
1	alice	1000.00	5000.00	0.0350
2	bob	1000.00	5000.00	0.0350
3	charlie	1000.00	5000.00	0.0350
-- !result
INSERT INTO users_pk_table (user_id, username, account_balance) VALUES (1, 'alice_updated', 2500.00);
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (2, 'bob_updated', 10000.00);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_updated	2500.00	5000.00	0.0350
2	bob_updated	1000.00	10000.00	0.0350
3	charlie	1000.00	5000.00	0.0350
-- !result
ALTER TABLE users_pk_table ADD COLUMN reward_points DECIMAL(10, 2) DEFAULT '100.00';
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, reward_points) VALUES (1, 'alice_v2', 500.00);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	2500.00	5000.00	0.0350	500.00
2	bob_updated	1000.00	10000.00	0.0350	100.00
3	charlie	1000.00	5000.00	0.0350	100.00
4	david	1000.00	5000.00	0.0350	100.00
-- !result
UPDATE users_pk_table SET account_balance = DEFAULT, credit_limit = DEFAULT WHERE user_id = 3;
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	2500.00	5000.00	0.0350	500.00
2	bob_updated	1000.00	10000.00	0.0350	100.00
3	charlie	1000.00	5000.00	0.0350	100.00
4	david	1000.00	5000.00	0.0350	100.00
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
ALTER TABLE event_logs ADD COLUMN processing_time DECIMAL(8, 3) DEFAULT '1.500';
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	1.500
2	event_2	1.500
-- !result
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	1.500
2	event_2	1.500
3	event_3	1.500
-- !result
CREATE TABLE edge_case_decimals (
    id INT NOT NULL,
    -- Test different decimal precisions and scales
    dec_small DECIMAL(5, 2) DEFAULT '123.45',
    dec_medium DECIMAL(10, 4) DEFAULT '12345.6789',
    dec_large DECIMAL(18, 6) DEFAULT '123456789.123456',
    dec_zero DECIMAL(10, 2) DEFAULT '0.00',
    dec_negative DECIMAL(10, 2) DEFAULT '-999.99',
    dec_high_precision DECIMAL(38, 10) DEFAULT '12345678.1234567890',
    dec_no_fraction DECIMAL(10, 0) DEFAULT '12345',
    -- Test Decimal256 (precision 38-76)
    dec256 DECIMAL(50, 15) DEFAULT '12345678901234567890123456789012345.123456789012345'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO edge_case_decimals (id) VALUES (1), (2), (3);
-- result:
-- !result
SELECT * FROM edge_case_decimals ORDER BY id;
-- result:
1	123.45	12345.6789	123456789.123456	0.00	-999.99	12345678.1234567890	12345	12345678901234567890123456789012345.123456789012345
2	123.45	12345.6789	123456789.123456	0.00	-999.99	12345678.1234567890	12345	12345678901234567890123456789012345.123456789012345
3	123.45	12345.6789	123456789.123456	0.00	-999.99	12345678.1234567890	12345	12345678901234567890123456789012345.123456789012345
-- !result
SELECT 
    id,
    CASE WHEN dec_small = 123.45 THEN 'PASS' ELSE 'FAIL' END as test_small,
    CASE WHEN dec_medium = 12345.6789 THEN 'PASS' ELSE 'FAIL' END as test_medium,
    CASE WHEN dec_large = 123456789.123456 THEN 'PASS' ELSE 'FAIL' END as test_large,
    CASE WHEN dec_zero = 0.00 THEN 'PASS' ELSE 'FAIL' END as test_zero,
    CASE WHEN dec_negative = -999.99 THEN 'PASS' ELSE 'FAIL' END as test_negative,
    CASE WHEN dec_high_precision = 12345678.1234567890 THEN 'PASS' ELSE 'FAIL' END as test_high_precision,
    CASE WHEN dec_no_fraction = 12345 THEN 'PASS' ELSE 'FAIL' END as test_no_fraction,
    CASE WHEN dec256 = 12345678901234567890123456789012345.123456789012345 THEN 'PASS' ELSE 'FAIL' END as test_dec256
FROM edge_case_decimals
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS	PASS	PASS	PASS
-- !result
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    total_revenue DECIMAL(15, 2) SUM DEFAULT '0.00',
    avg_price DECIMAL(10, 2) REPLACE DEFAULT '0.00',
    max_discount DECIMAL(5, 2) MAX DEFAULT '0.00'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
-- result:
-- !result
ALTER TABLE sales_summary ADD COLUMN avg_tax DECIMAL(5, 4) REPLACE DEFAULT '0.0800';
-- result:
-- !result
SELECT * FROM sales_summary ORDER BY product_id, region;
-- result:
1	North	0.00	0.00	0.00	0.0800
2	South	0.00	0.00	0.00	0.0800
-- !result
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    unit_cost DECIMAL(10, 2) DEFAULT '50.00',
    selling_price DECIMAL(10, 2) DEFAULT '100.00'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
-- result:
-- !result
ALTER TABLE inventory_items ADD COLUMN markup_rate DECIMAL(5, 2) DEFAULT '20.00';
-- result:
-- !result
SELECT * FROM inventory_items ORDER BY item_id;
-- result:
1	widget	50.00	100.00	20.00
2	gadget	50.00	100.00	20.00
-- !result