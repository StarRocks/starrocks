-- name: test_boolean_default
-- description: Comprehensive test for boolean default values
-- Tests Fast Schema Evolution, Traditional Schema Change, and Primary Key partial updates

drop database if exists test_bool_comprehensive;
CREATE DATABASE test_bool_comprehensive;
USE test_bool_comprehensive;

-- ========================================================================
-- Test 1: Fast Schema Evolution (most common scenario)
-- ========================================================================
-- ALTER TABLE ADD COLUMN with existing data, then SELECT to read old segments

CREATE TABLE users_basic (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);

-- Insert data before adding boolean columns
INSERT INTO users_basic VALUES 
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie');

-- Add boolean columns with different defaults
ALTER TABLE users_basic ADD COLUMN flag_true BOOLEAN DEFAULT 'true';
ALTER TABLE users_basic ADD COLUMN flag_false BOOLEAN DEFAULT 'false';
ALTER TABLE users_basic ADD COLUMN flag_1 BOOLEAN DEFAULT '1';
ALTER TABLE users_basic ADD COLUMN flag_0 BOOLEAN DEFAULT '0';

-- Query old data with new columns
SELECT id, name, flag_true, flag_false, flag_1, flag_0 FROM users_basic ORDER BY id;

-- Verify values are correct
SELECT 
    id,
    CASE WHEN flag_true = 1 THEN 'PASS' ELSE 'FAIL' END as test_true,
    CASE WHEN flag_false = 0 THEN 'PASS' ELSE 'FAIL' END as test_false,
    CASE WHEN flag_1 = 1 THEN 'PASS' ELSE 'FAIL' END as test_1,
    CASE WHEN flag_0 = 0 THEN 'PASS' ELSE 'FAIL' END as test_0
FROM users_basic 
ORDER BY id;

-- Test with mixed old and new data
INSERT INTO users_basic VALUES (4, 'david', 0, 1, 0, 1);
SELECT * FROM users_basic ORDER BY id;


-- ========================================================================
-- Test 2: Traditional Schema Change (Adding KEY column forces data rewrite)
-- ========================================================================
-- Adding KEY column requires rewriting data, triggers SchemaChangeUtils

CREATE TABLE products_with_key (
    id INT NOT NULL,
    price INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);

-- Insert data
INSERT INTO products_with_key VALUES (1, 100), (2, 200), (3, 300);

-- Add boolean columns with traditional schema change
ALTER TABLE products_with_key ADD COLUMN active BOOLEAN DEFAULT 'true';
function: wait_alter_table_finish()

SELECT count(*) FROM products_with_key;

-- Test 3: Modify column type also forces schema change
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

INSERT INTO items_type_change VALUES (1, 10, 1), (2, 20, 0);

-- Add boolean column after type modification
ALTER TABLE items_type_change ADD COLUMN verified BOOLEAN DEFAULT '1';
function: wait_alter_table_finish()

SELECT count(*) FROM items_type_change;


-- ========================================================================
-- Test 4: Column UPSERT Mode (Primary Key with column mode)
-- ========================================================================
-- Tests partial_update_mode='column' with PRIMARY KEY table

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

-- Set to column mode for UPSERT behavior
SET partial_update_mode = 'column';

-- Insert NEW rows with partial columns
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');

-- Verify defaults were filled correctly
SELECT * FROM orders_column_mode ORDER BY order_id;

-- Insert with some columns specified
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (4, 'monitor', 0);
INSERT INTO orders_column_mode (order_id, product_name, total_amount) VALUES (5, 'keyboard', 200);

SELECT * FROM orders_column_mode ORDER BY order_id;

-- Add a new column and insert more rows
ALTER TABLE orders_column_mode ADD COLUMN is_delivered BOOLEAN DEFAULT '1';

INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (7, 'headset', 1);

SELECT * FROM orders_column_mode ORDER BY order_id;

-- Reset to default mode
SET partial_update_mode = 'auto';


-- ========================================================================
-- Test 5: Primary Key Partial Update (general case)
-- ========================================================================
-- Tests PRIMARY KEY table with partial column inserts/updates

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

-- Insert with partial columns (uses default partial_update_mode)
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');

-- Verify defaults
SELECT * FROM users_pk_table ORDER BY user_id;

-- Partial update (update only some columns)
INSERT INTO users_pk_table (user_id, username, credit_score) VALUES (1, 'alice_updated', 100);
INSERT INTO users_pk_table (user_id, username, is_active) VALUES (2, 'bob_updated', 0);

SELECT * FROM users_pk_table ORDER BY user_id;

-- Add new boolean column
ALTER TABLE users_pk_table ADD COLUMN is_premium BOOLEAN DEFAULT '1';

-- Insert new rows and partial update existing
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, is_premium) VALUES (1, 'alice_v2', 0);

SELECT * FROM users_pk_table ORDER BY user_id;

-- Test UPDATE with DEFAULT keyword
UPDATE users_pk_table SET is_active = DEFAULT, is_verified = DEFAULT WHERE user_id = 3;

SELECT * FROM users_pk_table ORDER BY user_id;


-- ========================================================================
-- Test 6: Combined test (PK table with ALTER and partial updates)
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

-- Add boolean column (reads old data with new schema)
ALTER TABLE event_logs ADD COLUMN is_processed BOOLEAN DEFAULT 'false';

-- Read old data with new column
SELECT * FROM event_logs ORDER BY log_id;

-- Insert new row with partial columns
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');

-- Read all data
SELECT * FROM event_logs ORDER BY log_id;


-- ========================================================================
-- Edge Cases: Verify all boolean representations work
-- ========================================================================

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

INSERT INTO edge_case_booleans (id) VALUES (1), (2), (3);

SELECT * FROM edge_case_booleans ORDER BY id;

-- Verify all evaluate correctly
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


-- ========================================================================
-- Test 7: Aggregate Table with Boolean Defaults
-- ========================================================================

CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    is_active BOOLEAN REPLACE DEFAULT 'true',
    sales_count BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");

INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');

-- Add boolean column
ALTER TABLE sales_summary ADD COLUMN is_verified BOOLEAN REPLACE DEFAULT 'false';

SELECT * FROM sales_summary ORDER BY product_id, region;


-- ========================================================================
-- Test 8: Unique Key Table with Boolean Defaults
-- ========================================================================

CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    in_stock BOOLEAN DEFAULT 'true'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");

INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');

ALTER TABLE inventory_items ADD COLUMN is_discontinued BOOLEAN DEFAULT '0';

SELECT * FROM inventory_items ORDER BY item_id;
