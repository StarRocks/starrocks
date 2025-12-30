-- name: test_boolean_default
drop database if exists test_bool_comprehensive;
-- result:
-- !result
CREATE DATABASE test_bool_comprehensive;
-- result:
-- !result
USE test_bool_comprehensive;
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
ALTER TABLE users_basic ADD COLUMN flag_true BOOLEAN DEFAULT 'true';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN flag_false BOOLEAN DEFAULT 'false';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN flag_1 BOOLEAN DEFAULT '1';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN flag_0 BOOLEAN DEFAULT '0';
-- result:
-- !result
SELECT id, name, flag_true, flag_false, flag_1, flag_0 FROM users_basic ORDER BY id;
-- result:
1	alice	1	0	1	0
2	bob	1	0	1	0
3	charlie	1	0	1	0
-- !result
SELECT 
    id,
    CASE WHEN flag_true = 1 THEN 'PASS' ELSE 'FAIL' END as test_true,
    CASE WHEN flag_false = 0 THEN 'PASS' ELSE 'FAIL' END as test_false,
    CASE WHEN flag_1 = 1 THEN 'PASS' ELSE 'FAIL' END as test_1,
    CASE WHEN flag_0 = 0 THEN 'PASS' ELSE 'FAIL' END as test_0
FROM users_basic 
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS
-- !result
INSERT INTO users_basic VALUES (4, 'david', 0, 1, 0, 1);
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice	1	0	1	0
2	bob	1	0	1	0
3	charlie	1	0	1	0
4	david	0	1	0	1
-- !result
CREATE TABLE products_with_key (
    id INT NOT NULL,
    price INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
-- result:
-- !result
INSERT INTO products_with_key VALUES (1, 100), (2, 200), (3, 300);
-- result:
-- !result
ALTER TABLE products_with_key ADD COLUMN active BOOLEAN DEFAULT 'true';
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT count(*) FROM products_with_key;
-- result:
3
-- !result
CREATE TABLE items_type_change (
    id INT NOT NULL,
    quantity SMALLINT,
    available BOOLEAN DEFAULT 'true'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
-- result:
-- !result
INSERT INTO items_type_change VALUES (1, 10, 1), (2, 20, 0);
-- result:
-- !result
ALTER TABLE items_type_change ADD COLUMN verified BOOLEAN DEFAULT '1';
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
SELECT count(*) FROM items_type_change;
-- result:
2
-- !result
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    is_paid BOOLEAN DEFAULT 'true',
    is_shipped BOOLEAN DEFAULT 'false',
    total_amount INT DEFAULT '100'
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
1	laptop	1	0	100
2	phone	1	0	100
3	tablet	1	0	100
-- !result
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (4, 'monitor', 0);
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name, total_amount) VALUES (5, 'keyboard', 200);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	laptop	1	0	100
2	phone	1	0	100
3	tablet	1	0	100
4	monitor	0	0	100
5	keyboard	1	0	200
-- !result
ALTER TABLE orders_column_mode ADD COLUMN is_delivered BOOLEAN DEFAULT '1';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (7, 'headset', 1);
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	laptop	1	0	100	1
2	phone	1	0	100	1
3	tablet	1	0	100	1
4	monitor	0	0	100	1
5	keyboard	1	0	200	1
6	mouse	1	0	100	1
7	headset	1	0	100	1
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT 'true',
    is_verified BOOLEAN DEFAULT 'false',
    credit_score INT DEFAULT '0'
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
1	alice	1	0	0
2	bob	1	0	0
3	charlie	1	0	0
-- !result
INSERT INTO users_pk_table (user_id, username, credit_score) VALUES (1, 'alice_updated', 100);
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, is_active) VALUES (2, 'bob_updated', 0);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_updated	1	0	100
2	bob_updated	0	0	0
3	charlie	1	0	0
-- !result
ALTER TABLE users_pk_table ADD COLUMN is_premium BOOLEAN DEFAULT '1';
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, is_premium) VALUES (1, 'alice_v2', 0);
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	1	0	100	0
2	bob_updated	0	0	0	1
3	charlie	1	0	0	1
4	david	1	0	0	1
-- !result
UPDATE users_pk_table SET is_active = DEFAULT, is_verified = DEFAULT WHERE user_id = 3;
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	1	0	100	0
2	bob_updated	0	0	0	1
3	charlie	1	0	0	1
4	david	1	0	0	1
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
ALTER TABLE event_logs ADD COLUMN is_processed BOOLEAN DEFAULT 'false';
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	0
2	event_2	0
-- !result
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	0
2	event_2	0
3	event_3	0
-- !result
CREATE TABLE edge_case_booleans (
    id INT NOT NULL,
    -- Test all valid boolean default representations
    bool_true BOOLEAN DEFAULT 'true',
    bool_false BOOLEAN DEFAULT 'false',
    bool_1 BOOLEAN DEFAULT '1',
    bool_0 BOOLEAN DEFAULT '0'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO edge_case_booleans (id) VALUES (1), (2), (3);
-- result:
-- !result
SELECT * FROM edge_case_booleans ORDER BY id;
-- result:
1	1	0	1	0
2	1	0	1	0
3	1	0	1	0
-- !result
SELECT 
    id,
    bool_true = 1 as true_is_1,
    bool_false = 0 as false_is_0,
    bool_1 = 1 as one_is_1,
    bool_0 = 0 as zero_is_0,
    CASE 
        WHEN bool_true = 1 AND bool_false = 0 AND bool_1 = 1 AND bool_0 = 0 
        THEN 'ALL_PASS' 
        ELSE 'FAIL' 
    END as overall_result
FROM edge_case_booleans
ORDER BY id;
-- result:
1	1	1	1	1	ALL_PASS
2	1	1	1	1	ALL_PASS
3	1	1	1	1	ALL_PASS
-- !result
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    is_active BOOLEAN REPLACE DEFAULT 'true',
    sales_count BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
-- result:
-- !result
ALTER TABLE sales_summary ADD COLUMN is_verified BOOLEAN REPLACE DEFAULT 'false';
-- result:
-- !result
SELECT * FROM sales_summary ORDER BY product_id, region;
-- result:
1	North	1	0	0
2	South	1	0	0
-- !result
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    in_stock BOOLEAN DEFAULT 'true'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
-- result:
-- !result
ALTER TABLE inventory_items ADD COLUMN is_discontinued BOOLEAN DEFAULT '0';
-- result:
-- !result
SELECT * FROM inventory_items ORDER BY item_id;
-- result:
1	widget	1	0
2	gadget	1	0
-- !result