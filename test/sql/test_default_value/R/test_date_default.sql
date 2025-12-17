-- name: test_date_default
drop database if exists test_date_comprehensive;
-- result:
-- !result
CREATE DATABASE test_date_comprehensive;
-- result:
-- !result
USE test_date_comprehensive;
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
ALTER TABLE users_basic ADD COLUMN birth_date DATE DEFAULT '2000-01-01';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN created_at DATETIME DEFAULT '2024-01-01 00:00:00';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN updated_at DATETIME DEFAULT '2024-12-17 10:00:00';
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
2	bob	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
3	charlie	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
-- !result
SELECT 
    id,
    CASE WHEN birth_date = '2000-01-01' THEN 'PASS' ELSE 'FAIL' END as test_date,
    CASE WHEN created_at = '2024-01-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END as test_datetime1,
    CASE WHEN updated_at = '2024-12-17 10:00:00' THEN 'PASS' ELSE 'FAIL' END as test_datetime2
FROM users_basic 
ORDER BY id;
-- result:
1	PASS	PASS	PASS
2	PASS	PASS	PASS
3	PASS	PASS	PASS
-- !result
INSERT INTO users_basic VALUES (4, 'david', '1995-05-15', '2024-06-01 12:30:00', '2024-12-18 15:45:30');
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
2	bob	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
3	charlie	2000-01-01	2024-01-01 00:00:00	2024-12-17 10:00:00
4	david	1995-05-15	2024-06-01 12:30:00	2024-12-18 15:45:30
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
ALTER TABLE products_with_key 
    ADD COLUMN launch_date DATE DEFAULT '2024-01-01',
    ADD COLUMN last_updated DATETIME DEFAULT '2024-12-17 00:00:00';
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT count(*) FROM products_with_key ORDER BY id;
-- result:
E: (1064, "Getting analyzing error at line 1, column 48. Detail message: '`test_date_comprehensive`.`products_with_key`.`id`' must be an aggregate expression or appear in GROUP BY clause.")
-- !result
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    order_date DATE DEFAULT '2024-01-01',
    shipped_date DATE DEFAULT '2024-01-02',
    created_at DATETIME DEFAULT '2024-01-01 00:00:00',
    updated_at DATETIME DEFAULT '2024-01-01 00:00:00'
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
1	alice	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
2	bob	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
3	charlie	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (4, 'david', '2024-06-15');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, created_at) VALUES (5, 'eve', '2024-07-20 10:30:00');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
2	bob	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
3	charlie	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
4	david	2024-06-15	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00
5	eve	2024-01-01	2024-01-02	2024-07-20 10:30:00	2024-01-01 00:00:00
-- !result
ALTER TABLE orders_column_mode ADD COLUMN delivery_date DATE DEFAULT '2024-01-10';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (7, 'grace', '2024-08-01');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
2	bob	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
3	charlie	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
4	david	2024-06-15	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
5	eve	2024-01-01	2024-01-02	2024-07-20 10:30:00	2024-01-01 00:00:00	2024-01-10
6	frank	2024-01-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
7	grace	2024-08-01	2024-01-02	2024-01-01 00:00:00	2024-01-01 00:00:00	2024-01-10
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    registered_date DATE DEFAULT '2024-01-01',
    last_login DATETIME DEFAULT '2024-01-01 00:00:00',
    birth_date DATE DEFAULT '2000-01-01'
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
1	alice	2024-01-01	2024-01-01 00:00:00	2000-01-01
2	bob	2024-01-01	2024-01-01 00:00:00	2000-01-01
3	charlie	2024-01-01	2024-01-01 00:00:00	2000-01-01
-- !result
INSERT INTO users_pk_table (user_id, username, registered_date) VALUES (1, 'alice_updated', '2024-06-01');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, last_login) VALUES (2, 'bob_updated', '2024-12-17 10:30:00');
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_updated	2024-06-01	2024-01-01 00:00:00	2000-01-01
2	bob_updated	2024-01-01	2024-12-17 10:30:00	2000-01-01
3	charlie	2024-01-01	2024-01-01 00:00:00	2000-01-01
-- !result
ALTER TABLE users_pk_table ADD COLUMN account_expires DATE DEFAULT '2025-12-31';
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, account_expires) VALUES (1, 'alice_v2', '2026-06-30');
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	2024-06-01	2024-01-01 00:00:00	2000-01-01	2026-06-30
2	bob_updated	2024-01-01	2024-12-17 10:30:00	2000-01-01	2025-12-31
3	charlie	2024-01-01	2024-01-01 00:00:00	2000-01-01	2025-12-31
4	david	2024-01-01	2024-01-01 00:00:00	2000-01-01	2025-12-31
-- !result
UPDATE users_pk_table SET registered_date = DEFAULT, last_login = DEFAULT WHERE user_id = 3;
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	2024-06-01	2024-01-01 00:00:00	2000-01-01	2026-06-30
2	bob_updated	2024-01-01	2024-12-17 10:30:00	2000-01-01	2025-12-31
3	charlie	2024-01-01	2024-01-01 00:00:00	2000-01-01	2025-12-31
4	david	2024-01-01	2024-01-01 00:00:00	2000-01-01	2025-12-31
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
ALTER TABLE event_logs ADD COLUMN event_date DATE DEFAULT '2024-01-01';
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	2024-01-01
2	event_2	2024-01-01
-- !result
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	2024-01-01
2	event_2	2024-01-01
3	event_3	2024-01-01
-- !result
CREATE TABLE edge_case_dates (
    id INT NOT NULL,
    -- Test different date/datetime defaults
    date_early DATE DEFAULT '1970-01-01',
    date_recent DATE DEFAULT '2024-12-17',
    datetime_midnight DATETIME DEFAULT '2024-01-01 00:00:00',
    datetime_noon DATETIME DEFAULT '2024-06-15 12:00:00',
    datetime_full DATETIME DEFAULT '2024-12-17 23:59:59'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO edge_case_dates (id) VALUES (1), (2), (3);
-- result:
-- !result
SELECT * FROM edge_case_dates ORDER BY id;
-- result:
1	1970-01-01	2024-12-17	2024-01-01 00:00:00	2024-06-15 12:00:00	2024-12-17 23:59:59
2	1970-01-01	2024-12-17	2024-01-01 00:00:00	2024-06-15 12:00:00	2024-12-17 23:59:59
3	1970-01-01	2024-12-17	2024-01-01 00:00:00	2024-06-15 12:00:00	2024-12-17 23:59:59
-- !result
SELECT 
    id,
    CASE WHEN date_early = '1970-01-01' THEN 'PASS' ELSE 'FAIL' END as test_early,
    CASE WHEN date_recent = '2024-12-17' THEN 'PASS' ELSE 'FAIL' END as test_recent,
    CASE WHEN datetime_midnight = '2024-01-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END as test_midnight,
    CASE WHEN datetime_noon = '2024-06-15 12:00:00' THEN 'PASS' ELSE 'FAIL' END as test_noon,
    CASE WHEN datetime_full = '2024-12-17 23:59:59' THEN 'PASS' ELSE 'FAIL' END as test_full
FROM edge_case_dates
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS
-- !result
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_sale_date DATE REPLACE DEFAULT '2024-01-01',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
-- result:
-- !result
ALTER TABLE sales_summary ADD COLUMN first_sale_date DATE REPLACE DEFAULT '2023-01-01';
-- result:
-- !result
SELECT * FROM sales_summary ORDER BY product_id, region;
-- result:
1	North	2024-01-01	0	2023-01-01
2	South	2024-01-01	0	2023-01-01
-- !result
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    added_date DATE DEFAULT '2024-01-01',
    last_checked DATETIME DEFAULT '2024-01-01 00:00:00'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
-- result:
-- !result
ALTER TABLE inventory_items ADD COLUMN expiry_date DATE DEFAULT '2025-12-31';
-- result:
-- !result
SELECT * FROM inventory_items ORDER BY item_id;
-- result:
1	widget	2024-01-01	2024-01-01 00:00:00	2025-12-31
2	gadget	2024-01-01	2024-01-01 00:00:00	2025-12-31
-- !result