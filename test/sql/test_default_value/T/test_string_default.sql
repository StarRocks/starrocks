-- name: test_string_default
-- description: Comprehensive test for string type default values
-- Tests Fast Schema Evolution, Traditional Schema Change, and Primary Key partial updates

drop database if exists test_string_comprehensive;
CREATE DATABASE test_string_comprehensive;
USE test_string_comprehensive;

-- ========================================================================
-- Test 1: Fast Schema Evolution (most common scenario)
-- ========================================================================

CREATE TABLE users_basic (
    id INT NOT NULL,
    email VARCHAR(100)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);

-- Insert data before adding string columns
INSERT INTO users_basic VALUES 
    (1, 'alice@example.com'),
    (2, 'bob@example.com'),
    (3, 'charlie@example.com');

-- Add string columns with different defaults
ALTER TABLE users_basic ADD COLUMN status VARCHAR(20) DEFAULT 'active';
ALTER TABLE users_basic ADD COLUMN role VARCHAR(20) DEFAULT 'user';
ALTER TABLE users_basic ADD COLUMN country CHAR(2) DEFAULT 'US';
ALTER TABLE users_basic ADD COLUMN notes STRING DEFAULT 'no notes';

-- Query old data with new columns
SELECT * FROM users_basic ORDER BY id;

-- Verify values are correct
SELECT 
    id,
    CASE WHEN status = 'active' THEN 'PASS' ELSE 'FAIL' END as test_status,
    CASE WHEN role = 'user' THEN 'PASS' ELSE 'FAIL' END as test_role,
    CASE WHEN country = 'US' THEN 'PASS' ELSE 'FAIL' END as test_country,
    CASE WHEN notes = 'no notes' THEN 'PASS' ELSE 'FAIL' END as test_notes
FROM users_basic 
ORDER BY id;

-- Test with mixed old and new data
INSERT INTO users_basic VALUES (4, 'david@example.com', 'inactive', 'admin', 'CN', 'important user');
SELECT * FROM users_basic ORDER BY id;


-- ========================================================================
-- Test 2: Traditional Schema Change
-- ========================================================================

CREATE TABLE products_with_key (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);

-- Insert data
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');

-- Add string columns with traditional schema change
ALTER TABLE products_with_key ADD COLUMN brand VARCHAR(50) DEFAULT 'no brand';
function: wait_alter_table_finish()

SELECT count(*) FROM products_with_key;


-- ========================================================================
-- Test 3: Column UPSERT Mode (Primary Key with column mode)
-- ========================================================================

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

-- Set to column mode for UPSERT behavior
SET partial_update_mode = 'column';

-- Insert NEW rows with partial columns
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');

-- Verify defaults were filled correctly
SELECT * FROM orders_column_mode ORDER BY order_id;

-- Insert with some columns specified
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (4, 'david', 'shipped');
INSERT INTO orders_column_mode (order_id, customer_name, payment_method) VALUES (5, 'eve', 'credit_card');

SELECT * FROM orders_column_mode ORDER BY order_id;

-- Add a new column and insert more rows
ALTER TABLE orders_column_mode ADD COLUMN tracking_number VARCHAR(50) DEFAULT 'N/A';

INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
INSERT INTO orders_column_mode (order_id, customer_name, status) VALUES (7, 'grace', 'delivered');

SELECT * FROM orders_column_mode ORDER BY order_id;

-- Reset to default mode
SET partial_update_mode = 'auto';


-- ========================================================================
-- Test 4: Primary Key Partial Update (general case)
-- ========================================================================

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

-- Insert with partial columns
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');

-- Verify defaults
SELECT * FROM users_pk_table ORDER BY user_id;

-- Partial update
INSERT INTO users_pk_table (user_id, username, status) VALUES (1, 'alice_updated', 'premium');
INSERT INTO users_pk_table (user_id, username, role) VALUES (2, 'bob_updated', 'admin');

SELECT * FROM users_pk_table ORDER BY user_id;

-- Add new string column
ALTER TABLE users_pk_table ADD COLUMN email VARCHAR(100) DEFAULT 'unknown@example.com';

-- Insert new rows and partial update existing
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, email) VALUES (1, 'alice_v2', 'alice@example.com');

SELECT * FROM users_pk_table ORDER BY user_id;

-- Test UPDATE with DEFAULT keyword
UPDATE users_pk_table SET status = DEFAULT, role = DEFAULT WHERE user_id = 3;

SELECT * FROM users_pk_table ORDER BY user_id;


-- ========================================================================
-- Test 5: Combined test (PK table with ALTER and partial updates)
-- ========================================================================

CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);

-- Insert initial data
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');

-- Add string column (reads old data with new schema)
ALTER TABLE event_logs ADD COLUMN severity VARCHAR(20) DEFAULT 'INFO';

-- Read old data with new column
SELECT * FROM event_logs ORDER BY log_id;

-- Insert new row with partial columns
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');

-- Read all data
SELECT * FROM event_logs ORDER BY log_id;


-- ========================================================================
-- Edge Cases: Verify different string types and edge values
-- ========================================================================

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

INSERT INTO edge_case_strings (id) VALUES (1), (2), (3);

SELECT * FROM edge_case_strings ORDER BY id;

-- Verify all evaluate correctly
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


-- ========================================================================
-- Test 6: Aggregate Table with String Defaults
-- ========================================================================

CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_status VARCHAR(50) REPLACE DEFAULT 'unknown',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");

INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');

-- Add string column
ALTER TABLE sales_summary ADD COLUMN last_updated_by VARCHAR(50) REPLACE DEFAULT 'system';

SELECT * FROM sales_summary ORDER BY product_id, region;


-- ========================================================================
-- Test 7: Unique Key Table with String Defaults
-- ========================================================================

CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    location VARCHAR(50) DEFAULT 'warehouse',
    supplier VARCHAR(50) DEFAULT 'default supplier'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");

INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');

ALTER TABLE inventory_items ADD COLUMN barcode VARCHAR(50) DEFAULT 'NO_BARCODE';

SELECT * FROM inventory_items ORDER BY item_id;
