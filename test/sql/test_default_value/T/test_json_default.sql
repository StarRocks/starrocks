-- name: test_json_default
-- Test JSON column default value support across various scenarios
-- Covers fast/traditional schema evolution, extended columns, and PRIMARY KEY tables

DROP DATABASE IF EXISTS test_json_default_db;
CREATE DATABASE test_json_default_db;
USE test_json_default_db;

-- ====================================================================================
-- SECTION 1: Basic JSON Default Values
-- ====================================================================================

-- Test: All JSON value types as defaults
CREATE TABLE basic_json_types (
    id INT NOT NULL,
    json_object JSON DEFAULT '{"status": "active", "count": 0}',
    json_array JSON DEFAULT '[1, 2, 3]',
    json_string JSON DEFAULT '"hello"',
    json_number JSON DEFAULT '42',
    json_boolean JSON DEFAULT 'true',
    json_null JSON DEFAULT 'null',
    empty_object JSON DEFAULT '{}',
    empty_array JSON DEFAULT '[]',
    empty_string JSON DEFAULT '',
    no_default JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Insert using all defaults
INSERT INTO basic_json_types (id) VALUES (1);

-- Verify all default values
SELECT * FROM basic_json_types ORDER BY id;

-- Test: Comparing columns with and without defaults
CREATE TABLE with_default (
    id INT,
    data JSON DEFAULT '{"default": true}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

CREATE TABLE without_default (
    id INT,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO with_default (id) VALUES (1);
INSERT INTO without_default (id) VALUES (1);

-- Compare: default value vs NULL
SELECT 
    'with_default' AS table_name,
    data,
    data IS NULL AS is_null
FROM with_default
UNION ALL
SELECT 
    'without_default',
    data,
    data IS NULL
FROM without_default order by 1;

-- Test: Empty string handling
CREATE TABLE empty_string_test (
    id INT,
    empty_unquoted JSON DEFAULT '',
    empty_quoted JSON DEFAULT '""'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO empty_string_test (id) VALUES (1);
INSERT INTO empty_string_test VALUES (2, '', '""');

SELECT 
    id,
    empty_unquoted,
    empty_quoted,
    empty_unquoted = empty_quoted AS are_equal
FROM empty_string_test
ORDER BY id;

-- ====================================================================================
-- SECTION 2: ALTER TABLE ADD COLUMN - Fast Schema Evolution
-- ====================================================================================

-- Test: Add JSON column to existing table (fast schema evolution)
CREATE TABLE fast_schema_change (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

-- Insert data before adding JSON column
INSERT INTO fast_schema_change VALUES (1, 'alice'), (2, 'bob');

-- Add JSON column with default value
ALTER TABLE fast_schema_change ADD COLUMN metadata JSON DEFAULT '{"version": 1, "enabled": true}';

-- Old rows should use default value
SELECT id, name, metadata FROM fast_schema_change ORDER BY id;

-- New rows can have actual values
INSERT INTO fast_schema_change VALUES (3, 'charlie', '{"version": 2, "enabled": false}');

SELECT id, name, metadata FROM fast_schema_change ORDER BY id;

-- ====================================================================================
-- SECTION 3: ALTER TABLE ADD COLUMN - Traditional Schema Change
-- ====================================================================================

-- Test: Add JSON column with data rewrite (traditional schema change)
CREATE TABLE traditional_schema_change (
    id INT NOT NULL,
    value INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "false");

INSERT INTO traditional_schema_change VALUES (1, 100), (2, 200);

-- Add JSON column - triggers full data rewrite
ALTER TABLE traditional_schema_change ADD COLUMN metadata JSON DEFAULT '{"source": "migration", "timestamp": 0}';

function: wait_alter_table_finish()

SELECT count(*) FROM traditional_schema_change;

-- ====================================================================================
-- SECTION 4: Extended Column with JSON Functions (FlatJSON Optimization)
-- ====================================================================================

-- Test: JSON subfield extraction with extended columns
-- When using get_json_int/bool/string functions on JSON columns added via ALTER TABLE,
-- StarRocks creates "Extended Columns" as a FlatJSON optimization.
-- Old rows (before ALTER) must correctly use the JSON default value.

CREATE TABLE extended_column_basic (
    id INT NOT NULL,
    user_data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Insert data before adding new JSON column
INSERT INTO extended_column_basic VALUES 
    (1, '{"user": {"name": "alice", "age": 25}}'),
    (2, '{"user": {"name": "bob", "age": 30}}');

-- Add NEW JSON column with nested default value
ALTER TABLE extended_column_basic ADD COLUMN profile JSON DEFAULT '{"level": 1, "vip": false, "tags": ["default"]}';

-- Query JSON subfields using get_json_* functions
-- This creates Extended Columns (e.g., profile.level, profile.vip)
-- Old rows should correctly extract values from the default JSON
SELECT 
    id,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.tags[0]') AS first_tag
FROM extended_column_basic
ORDER BY id;

-- Also test with json_query function
SELECT 
    id,
    json_query(profile, '$.tags') AS tags,
    cast(get_json_int(profile, '$.level') AS INT) AS level
FROM extended_column_basic
ORDER BY id;

-- Insert new data with actual values
INSERT INTO extended_column_basic VALUES 
    (3, '{"user": {"name": "charlie", "age": 35}}', '{"level": 10, "vip": true, "tags": ["premium"]}');

-- Query again: mix of default values (rows 1-2) and actual values (row 3)
SELECT 
    id,
    get_json_string(user_data, '$.user.name') AS user_name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.tags[0]') AS first_tag
FROM extended_column_basic
ORDER BY id;

-- ====================================================================================
-- SECTION 4.1: Extended subcolumn inherits DEFAULT from JSON parent (type coverage)
-- ====================================================================================

CREATE TABLE extended_default_inherit_types (
    k1 INT
) DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");

-- Old rows before ALTER (missing json_col in old segments)
INSERT INTO extended_default_inherit_types SELECT 1;

ALTER TABLE extended_default_inherit_types
ADD COLUMN json_col JSON DEFAULT '{
  "i_str": "222",
  "i_num": 223,
  "b_str": "true",
  "b_bool": true,
  "d_str": "1.25",
  "d_num": 2.5,
  "s": "hello",
  "nullv": null,
  "obj": {"x": "7"},
  "arr": ["9"]
}';

-- Old row: should read inherited defaults from JSON parent
SELECT
  k1,
  get_json_int(json_col, '$.i_str') AS i_str,
  get_json_int(json_col, '$.i_num') AS i_num,
  get_json_bool(json_col, '$.b_str') AS b_str,
  get_json_bool(json_col, '$.b_bool') AS b_bool,
  get_json_double(json_col, '$.d_str') AS d_str,
  get_json_double(json_col, '$.d_num') AS d_num,
  get_json_string(json_col, '$.s') AS s,
  get_json_int(json_col, '$.nullv') AS nullv_int,
  get_json_int(json_col, '$.obj') AS obj_int,
  get_json_int(json_col, '$.arr[0]') AS arr0_int
FROM extended_default_inherit_types
ORDER BY k1;

INSERT INTO extended_default_inherit_types(k1) SELECT 2;

INSERT INTO extended_default_inherit_types
SELECT 3, '{"i_str":"333","i_num":334,"b_str":"false","b_bool":false,"d_str":"3.75","d_num":4.5,"s":"world","nullv":null,"obj":{"x":"8"},"arr":["10"]}';

SELECT
  k1,
  get_json_int(json_col, '$.i_str') AS i_str,
  get_json_int(json_col, '$.i_num') AS i_num,
  get_json_bool(json_col, '$.b_str') AS b_str,
  get_json_bool(json_col, '$.b_bool') AS b_bool,
  get_json_double(json_col, '$.d_str') AS d_str,
  get_json_double(json_col, '$.d_num') AS d_num,
  get_json_string(json_col, '$.s') AS s,
  get_json_int(json_col, '$.nullv') AS nullv_int,
  get_json_int(json_col, '$.obj') AS obj_int,
  get_json_int(json_col, '$.arr[0]') AS arr0_int
FROM extended_default_inherit_types
ORDER BY k1;

-- ====================================================================================
-- SECTION 5: Extended Column - Comprehensive Scenarios
-- ====================================================================================

-- Test 5.1: Complex nested JSON with all function types
CREATE TABLE extended_complex_types (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_complex_types VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');

ALTER TABLE extended_complex_types 
ADD COLUMN profile JSON DEFAULT '{"level": 10, "vip": true, "score": 95.5, "tags": ["gold", "premium"], "meta": {"city": "Beijing", "age": 25}}';

-- Test all JSON extraction function types
SELECT 
    id,
    name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    cast(get_json_double(profile, '$.score') AS DOUBLE) AS score,
    get_json_string(profile, '$.tags[0]') AS first_tag,
    get_json_string(profile, '$.tags[1]') AS second_tag,
    get_json_string(profile, '$.meta.city') AS city,
    cast(get_json_int(profile, '$.meta.age') AS INT) AS age
FROM extended_complex_types
ORDER BY id;

-- Mix default and actual values
INSERT INTO extended_complex_types VALUES 
    (4, 'david', '{"level": 99, "vip": false, "score": 88.8, "tags": ["silver"], "meta": {"city": "Shanghai", "age": 30}}');

SELECT 
    id,
    name,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip
FROM extended_complex_types
ORDER BY id;

-- Test 5.2: Multiple JSON columns with different defaults
CREATE TABLE extended_multi_json (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_multi_json VALUES (1, 'user1'), (2, 'user2');

ALTER TABLE extended_multi_json ADD COLUMN config JSON DEFAULT '{"theme": "dark", "lang": "en"}';

ALTER TABLE extended_multi_json ADD COLUMN stats JSON DEFAULT '{"views": 100, "likes": 50}';

-- Query subfields from multiple JSON columns
SELECT 
    id,
    name,
    get_json_string(config, '$.theme') AS theme,
    get_json_string(config, '$.lang') AS lang,
    cast(get_json_int(stats, '$.views') AS INT) AS views,
    cast(get_json_int(stats, '$.likes') AS INT) AS likes
FROM extended_multi_json
ORDER BY id;

-- Test 5.3: JSON with null values
CREATE TABLE extended_null_values (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_null_values VALUES (1), (2);

ALTER TABLE extended_null_values ADD COLUMN data JSON DEFAULT '{"value": null, "count": 0, "name": "test"}';

SELECT 
    id,
    get_json_string(data, '$.value') AS value_field,
    cast(get_json_int(data, '$.count') AS INT) AS count_field,
    get_json_string(data, '$.name') AS name_field
FROM extended_null_values
ORDER BY id;

-- Test 5.4: Empty arrays and objects
CREATE TABLE extended_empty_structures (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_empty_structures VALUES (1), (2);

ALTER TABLE extended_empty_structures ADD COLUMN info JSON DEFAULT '{"tags": [], "meta": {}}';

SELECT 
    id,
    json_query(info, '$.tags') AS tags,
    json_query(info, '$.meta') AS meta,
    get_json_string(info, '$.tags[0]') AS first_tag
FROM extended_empty_structures
ORDER BY id;

-- Test 5.5: Deep nested structures
CREATE TABLE extended_deep_nested (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_deep_nested VALUES (1), (2);

ALTER TABLE extended_deep_nested ADD COLUMN deep JSON DEFAULT '{"level1": {"level2": {"level3": {"value": 42, "flag": true}}}}';

SELECT 
    id,
    cast(get_json_int(deep, '$.level1.level2.level3.value') AS INT) AS nested_value,
    cast(get_json_bool(deep, '$.level1.level2.level3.flag') AS BOOLEAN) AS nested_flag,
    json_query(deep, '$.level1.level2') AS level2_obj
FROM extended_deep_nested
ORDER BY id;

-- Test 5.6: Arrays with multiple elements
CREATE TABLE extended_arrays (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_arrays VALUES (1), (2), (3);

ALTER TABLE extended_arrays ADD COLUMN items JSON DEFAULT '{"products": ["apple", "banana", "orange"], "prices": [1.5, 2.0, 1.8]}';

SELECT 
    id,
    get_json_string(items, '$.products[0]') AS product_0,
    get_json_string(items, '$.products[1]') AS product_1,
    get_json_string(items, '$.products[2]') AS product_2,
    cast(get_json_double(items, '$.prices[0]') AS DOUBLE) AS price_0,
    cast(get_json_double(items, '$.prices[1]') AS DOUBLE) AS price_1,
    json_query(items, '$.products') AS all_products
FROM extended_arrays
ORDER BY id;

-- Test 5.7: Function compatibility (json_query vs get_json_*)
CREATE TABLE extended_function_compat (
    id INT NOT NULL
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO extended_function_compat VALUES (1), (2);

ALTER TABLE extended_function_compat ADD COLUMN profile JSON DEFAULT '{"user": {"name": "Alice", "age": 25}, "score": 95}';

-- Both function styles should return consistent results
SELECT 
    id,
    json_query(profile, '$.user.name') AS name_via_query,
    get_json_string(profile, '$.user.name') AS name_via_get,
    json_query(profile, '$.score') AS score_via_query,
    cast(get_json_int(profile, '$.score') AS INT) AS score_via_get
FROM extended_function_compat
ORDER BY id;

-- ====================================================================================
-- SECTION 6: PRIMARY KEY Table Tests
-- ====================================================================================

-- Test 6.1: Basic PRIMARY KEY table with JSON defaults
-- This tests default value filling when inserting partial columns into PRIMARY KEY table
CREATE TABLE pk_basic_defaults (
    user_id INT NOT NULL,
    username VARCHAR(50),
    profile JSON DEFAULT '{"level": 1, "vip": false, "status": "active"}',
    settings JSON DEFAULT '{"notifications": true, "theme": "light"}'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Insert with partial columns (profile and settings should use defaults)
INSERT INTO pk_basic_defaults (user_id, username) VALUES (1, 'user1');
INSERT INTO pk_basic_defaults (user_id, username) VALUES (2, 'user2');

-- Verify defaults were used
SELECT 
    user_id,
    username,
    profile,
    settings
FROM pk_basic_defaults
ORDER BY user_id;

-- Query JSON subfields from default values
SELECT 
    user_id,
    username,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip,
    get_json_string(profile, '$.status') AS status,
    cast(get_json_bool(settings, '$.notifications') AS BOOLEAN) AS notifications
FROM pk_basic_defaults
ORDER BY user_id;

-- Insert with explicit values for comparison
INSERT INTO pk_basic_defaults VALUES 
    (3, 'user3', '{"level": 10, "vip": true, "status": "premium"}', '{"notifications": false, "theme": "dark"}');

-- Mix of default and explicit values
SELECT 
    user_id,
    username,
    cast(get_json_int(profile, '$.level') AS INT) AS level,
    cast(get_json_bool(profile, '$.vip') AS BOOLEAN) AS vip
FROM pk_basic_defaults
ORDER BY user_id;

-- Test 6.2: Column mode partial update
-- This tests that JSON default values are correctly filled when using
-- partial_update_mode = 'column' and inserting only some columns

CREATE TABLE pk_partial_update (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    config JSON DEFAULT '{"priority": "normal", "tracking": true}',
    metadata JSON DEFAULT '{"source": "web", "version": 1}'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES("replication_num" = "1");

-- Enable column mode for partial updates
SET partial_update_mode = 'column';

-- Insert rows with only some columns (others should use defaults)
INSERT INTO pk_partial_update (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (2, 'phone');
INSERT INTO pk_partial_update (order_id, product_name) VALUES (3, 'tablet');


-- Verify JSON defaults were correctly filled
SELECT 
    order_id,
    product_name,
    quantity,
    get_json_string(config, '$.priority') AS priority,
    cast(get_json_bool(config, '$.tracking') AS BOOLEAN) AS tracking,
    get_json_string(metadata, '$.source') AS source,
    cast(get_json_int(metadata, '$.version') AS INT) AS version
FROM pk_partial_update 
ORDER BY order_id;

-- Insert with partial JSON columns
INSERT INTO pk_partial_update (order_id, product_name, config) 
VALUES (4, 'monitor', '{"priority": "high", "tracking": false}');

INSERT INTO pk_partial_update (order_id, product_name, metadata) 
VALUES (5, 'keyboard', '{"source": "mobile", "version": 2}');


SELECT 
    order_id,
    product_name,
    get_json_string(config, '$.priority') AS priority,
    get_json_string(metadata, '$.source') AS source
FROM pk_partial_update 
ORDER BY order_id;

-- Add another JSON column after data exists
ALTER TABLE pk_partial_update ADD COLUMN extras JSON DEFAULT '{"discount": 0.0, "tax": 0.08}';

-- Insert with partial columns again
INSERT INTO pk_partial_update (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO pk_partial_update (order_id, product_name, quantity) VALUES (7, 'headset', 3);

-- Verify all JSON defaults including newly added column
SELECT 
    order_id,
    product_name,
    quantity,
    get_json_string(config, '$.priority') AS priority,
    get_json_string(metadata, '$.source') AS source,
    cast(get_json_double(extras, '$.discount') AS DOUBLE) AS discount,
    cast(get_json_double(extras, '$.tax') AS DOUBLE) AS tax
FROM pk_partial_update 
ORDER BY order_id;

-- Reset to default mode
SET partial_update_mode = 'auto';

-- ====================================================================================
-- SECTION 7: FlatJSON Field Absence Optimization
-- ====================================================================================

-- Test: Query JSON fields that don't exist in some segments
-- This tests FlatJSON's bloom filter optimization for quickly returning NULL
-- when a field is provably absent from a segment

CREATE TABLE flatjson_field_absence (
    id INT NOT NULL,
    data JSON
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Batch 1: Insert JSONs WITHOUT 'optional_field'
INSERT INTO flatjson_field_absence VALUES 
    (1, '{"name": "alice", "age": 25, "city": "Beijing"}'),
    (2, '{"name": "bob", "age": 30, "city": "Shanghai"}'),
    (3, '{"name": "charlie", "age": 35, "city": "Guangzhou"}');

-- Batch 2: Insert JSONs WITH 'optional_field'
INSERT INTO flatjson_field_absence VALUES 
    (4, '{"name": "david", "age": 40, "city": "Shenzhen", "optional_field": "value1"}'),
    (5, '{"name": "eve", "age": 45, "city": "Hangzhou", "optional_field": "value2"}');

-- Query optional field: NULL for batch 1, actual values for batch 2
SELECT 
    id,
    get_json_string(data, '$.name') AS name,
    get_json_string(data, '$.optional_field') AS optional_field_get,
    json_query(data, '$.optional_field') AS optional_field_query
FROM flatjson_field_absence
ORDER BY id;

-- Query completely missing field: NULL for all rows
SELECT 
    id,
    get_json_string(data, '$.completely_missing') AS missing_field
FROM flatjson_field_absence
ORDER BY id;
