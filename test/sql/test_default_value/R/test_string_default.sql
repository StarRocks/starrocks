-- name: test_string_default
drop database if exists test_string_comprehensive;
-- result:
-- !result
CREATE DATABASE test_string_comprehensive;
-- result:
-- !result
USE test_string_comprehensive;
-- result:
-- !result
CREATE TABLE users_basic (
    id INT NOT NULL,
    email VARCHAR(100)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
-- result:
-- !result
INSERT INTO users_basic VALUES 
    (1, 'alice@example.com'),
    (2, 'bob@example.com'),
    (3, 'charlie@example.com');
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN status VARCHAR(20) DEFAULT 'active';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN role VARCHAR(20) DEFAULT 'user';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN country CHAR(2) DEFAULT 'US';
-- result:
-- !result
ALTER TABLE users_basic ADD COLUMN notes STRING DEFAULT 'no notes';
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice@example.com	active	user	US	no notes
2	bob@example.com	active	user	US	no notes
3	charlie@example.com	active	user	US	no notes
-- !result
SELECT 
    id,
    CASE WHEN status = 'active' THEN 'PASS' ELSE 'FAIL' END as test_status,
    CASE WHEN role = 'user' THEN 'PASS' ELSE 'FAIL' END as test_role,
    CASE WHEN country = 'US' THEN 'PASS' ELSE 'FAIL' END as test_country,
    CASE WHEN notes = 'no notes' THEN 'PASS' ELSE 'FAIL' END as test_notes
FROM users_basic 
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS
-- !result
INSERT INTO users_basic VALUES (4, 'david@example.com', 'inactive', 'admin', 'CN', 'important user');
-- result:
-- !result
SELECT * FROM users_basic ORDER BY id;
-- result:
1	alice@example.com	active	user	US	no notes
2	bob@example.com	active	user	US	no notes
3	charlie@example.com	active	user	US	no notes
4	david@example.com	inactive	admin	CN	important user
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
ALTER TABLE products_with_key ADD COLUMN brand VARCHAR(50) DEFAULT 'no brand';
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
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    status VARCHAR(20) DEFAULT 'pending',
    payment_method VARCHAR(20) DEFAULT 'cash',
    shipping_address STRING DEFAULT 'not specified',
    notes VARCHAR(200) DEFAULT ''
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
1	alice	pending	cash	not specified	
2	bob	pending	cash	not specified	
3	charlie	pending	cash	not specified	
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (4, 'david', 'shipped');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, payment_method) VALUES (5, 'eve', 'credit_card');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	pending	cash	not specified	
2	bob	pending	cash	not specified	
3	charlie	pending	cash	not specified	
4	david	shipped	cash	not specified	
5	eve	pending	credit_card	not specified	
-- !result
ALTER TABLE orders_column_mode ADD COLUMN tracking_number VARCHAR(50) DEFAULT 'N/A';
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
-- result:
-- !result
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (7, 'grace', 'delivered');
-- result:
-- !result
SELECT * FROM orders_column_mode ORDER BY order_id;
-- result:
1	alice	pending	cash	not specified		N/A
2	bob	pending	cash	not specified		N/A
3	charlie	pending	cash	not specified		N/A
4	david	shipped	cash	not specified		N/A
5	eve	pending	credit_card	not specified		N/A
6	frank	pending	cash	not specified		N/A
7	grace	delivered	cash	not specified		N/A
-- !result
SET partial_update_mode = 'auto';
-- result:
-- !result
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    role VARCHAR(20) DEFAULT 'member',
    bio STRING DEFAULT 'No bio available'
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
1	alice	active	member	No bio available
2	bob	active	member	No bio available
3	charlie	active	member	No bio available
-- !result
INSERT INTO users_pk_table (user_id, username, status) VALUES (1, 'alice_updated', 'premium');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, role) VALUES (2, 'bob_updated', 'admin');
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_updated	premium	member	No bio available
2	bob_updated	active	admin	No bio available
3	charlie	active	member	No bio available
-- !result
ALTER TABLE users_pk_table ADD COLUMN email VARCHAR(100) DEFAULT 'unknown@example.com';
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
-- result:
-- !result
INSERT INTO users_pk_table (user_id, username, email) VALUES (1, 'alice_v2', 'alice@example.com');
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	premium	member	No bio available	alice@example.com
2	bob_updated	active	admin	No bio available	unknown@example.com
3	charlie	active	member	No bio available	unknown@example.com
4	david	active	member	No bio available	unknown@example.com
-- !result
UPDATE users_pk_table SET status = DEFAULT, role = DEFAULT WHERE user_id = 3;
-- result:
-- !result
SELECT * FROM users_pk_table ORDER BY user_id;
-- result:
1	alice_v2	premium	member	No bio available	alice@example.com
2	bob_updated	active	admin	No bio available	unknown@example.com
3	charlie	active	member	No bio available	unknown@example.com
4	david	active	member	No bio available	unknown@example.com
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
ALTER TABLE event_logs ADD COLUMN severity VARCHAR(20) DEFAULT 'INFO';
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	INFO
2	event_2	INFO
-- !result
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
-- result:
-- !result
SELECT * FROM event_logs ORDER BY log_id;
-- result:
1	event_1	INFO
2	event_2	INFO
3	event_3	INFO
-- !result
CREATE TABLE edge_case_strings (
    id INT NOT NULL,
    -- Test different string types with various defaults
    varchar_short VARCHAR(10) DEFAULT 'test',
    varchar_long VARCHAR(255) DEFAULT 'This is a longer default value for testing',
    char_fixed CHAR(5) DEFAULT 'ABCDE',
    string_type STRING DEFAULT 'String type default',
    empty_varchar VARCHAR(50) DEFAULT '',
    special_chars VARCHAR(100) DEFAULT 'Special: @#$%^&*()',
    unicode_str VARCHAR(100) DEFAULT '测试中文'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO edge_case_strings (id) VALUES (1), (2), (3);
-- result:
-- !result
SELECT * FROM edge_case_strings ORDER BY id;
-- result:
1	test	This is a longer default value for testing	ABCDE	String type default		Special: @#$%^&*()	测试中文
2	test	This is a longer default value for testing	ABCDE	String type default		Special: @#$%^&*()	测试中文
3	test	This is a longer default value for testing	ABCDE	String type default		Special: @#$%^&*()	测试中文
-- !result
SELECT 
    id,
    CASE WHEN varchar_short = 'test' THEN 'PASS' ELSE 'FAIL' END as test_short,
    CASE WHEN varchar_long = 'This is a longer default value for testing' THEN 'PASS' ELSE 'FAIL' END as test_long,
    CASE WHEN char_fixed = 'ABCDE' THEN 'PASS' ELSE 'FAIL' END as test_char,
    CASE WHEN string_type = 'String type default' THEN 'PASS' ELSE 'FAIL' END as test_string,
    CASE WHEN empty_varchar = '' THEN 'PASS' ELSE 'FAIL' END as test_empty,
    CASE WHEN special_chars = 'Special: @#$%^&*()' THEN 'PASS' ELSE 'FAIL' END as test_special,
    CASE WHEN unicode_str = '测试中文' THEN 'PASS' ELSE 'FAIL' END as test_unicode
FROM edge_case_strings
ORDER BY id;
-- result:
1	PASS	PASS	PASS	PASS	PASS	PASS	PASS
2	PASS	PASS	PASS	PASS	PASS	PASS	PASS
3	PASS	PASS	PASS	PASS	PASS	PASS	PASS
-- !result
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_status VARCHAR(50) REPLACE DEFAULT 'unknown',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
-- result:
-- !result
ALTER TABLE sales_summary ADD COLUMN last_updated_by VARCHAR(50) REPLACE DEFAULT 'system';
-- result:
-- !result
SELECT * FROM sales_summary ORDER BY product_id, region;
-- result:
1	North	unknown	0	system
2	South	unknown	0	system
-- !result
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    location VARCHAR(50) DEFAULT 'warehouse',
    supplier VARCHAR(50) DEFAULT 'default supplier'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
-- result:
-- !result
ALTER TABLE inventory_items ADD COLUMN barcode VARCHAR(50) DEFAULT 'NO_BARCODE';
-- result:
-- !result
SELECT * FROM inventory_items ORDER BY item_id;
-- result:
1	widget	warehouse	default supplier	NO_BARCODE
2	gadget	warehouse	default supplier	NO_BARCODE
-- !result