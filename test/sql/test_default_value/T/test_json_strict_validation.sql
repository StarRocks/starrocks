-- name: test_json_strict_validation
-- Test strict JSON validation for default values
-- using strict JSON parsing (rejects non-standard JSON syntax)

DROP DATABASE IF EXISTS test_json_strict_db;
CREATE DATABASE test_json_strict_db;
USE test_json_strict_db;

-- ============================================
-- Test 1: Valid Standard JSON - Should succeed
-- ============================================
CREATE TABLE test_json_strict_valid (
    id INT,
    data1 JSON DEFAULT '{"key": "value"}',
    data2 JSON DEFAULT '{"name": "Alice", "age": 30}',
    data3 JSON DEFAULT '{}',
    data4 JSON DEFAULT 'null',
    data5 JSON DEFAULT '""',
    data6 JSON DEFAULT '123',
    data7 JSON DEFAULT 'true'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Verify: Insert data and query
INSERT INTO test_json_strict_valid (id) VALUES (1);
SELECT * FROM test_json_strict_valid;

-- ============================================
-- Test 2: Unquoted Keys - Should fail
-- ============================================
-- Expected: ERROR 1064 (HY000): Invalid JSON format
CREATE TABLE test_json_strict_invalid_unquoted_key (
    id INT,
    data JSON DEFAULT '{key: "value"}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- ============================================
-- Test 3: Trailing Comma - Should fail
-- ============================================
-- Expected: ERROR 1064 (HY000): Invalid JSON format
CREATE TABLE test_json_strict_invalid_trailing_comma (
    id INT,
    data JSON DEFAULT '{"key": "value",}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- ============================================
-- Test 4: Single Quotes - Should fail
-- ============================================
-- Expected: ERROR 1064 (HY000): Invalid JSON format
CREATE TABLE test_json_strict_invalid_single_quote (
    id INT,
    data JSON DEFAULT "{'key': 'value'}"
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- ============================================
-- Test 5: JavaScript-style Comments - Should fail
-- ============================================
-- Expected: ERROR 1064 (HY000): Invalid JSON format
CREATE TABLE test_json_strict_invalid_comment (
    id INT,
    data JSON DEFAULT '{"key": "value" /* comment */}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- ============================================
-- Test 6: Complex Nested JSON - Should succeed
-- ============================================
CREATE TABLE test_json_strict_valid_complex (
    id INT,
    data JSON DEFAULT '{"user": {"name": "Bob", "age": 25, "address": {"city": "Beijing", "zip": "100000"}}, "tags": ["a", "b", "c"], "active": true, "score": 95.5}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Verify: Insert data and query
INSERT INTO test_json_strict_valid_complex (id) VALUES (1);
SELECT * FROM test_json_strict_valid_complex;

-- ============================================
-- Test 7: Deep Nested JSON - Should succeed
-- ============================================
CREATE TABLE test_json_strict_valid_nested (
    id INT,
    data JSON DEFAULT '{"level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}}}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Verify: Insert data and query
INSERT INTO test_json_strict_valid_nested (id) VALUES (1);
SELECT * FROM test_json_strict_valid_nested;

-- ============================================
-- Test 8: Array-based JSON - Should succeed
-- ============================================
CREATE TABLE test_json_strict_valid_array (
    id INT,
    data1 JSON DEFAULT '[1, 2, 3, 4, 5]',
    data2 JSON DEFAULT '[{"name": "Alice"}, {"name": "Bob"}]',
    data3 JSON DEFAULT '[[1, 2], [3, 4], [5, 6]]',
    data4 JSON DEFAULT '[]'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Verify: Insert data and query
INSERT INTO test_json_strict_valid_array (id) VALUES (1);
SELECT * FROM test_json_strict_valid_array;

-- ============================================
-- Test 9: Edge Cases - Should succeed
-- ============================================
CREATE TABLE test_json_strict_edge_cases (
    id INT,
    empty_obj JSON DEFAULT '{}',
    empty_arr JSON DEFAULT '[]',
    null_val JSON DEFAULT 'null',
    empty_str JSON DEFAULT '""',
    zero_num JSON DEFAULT '0',
    neg_num JSON DEFAULT '-123.45',
    bool_true JSON DEFAULT 'true',
    bool_false JSON DEFAULT 'false',
    unicode JSON DEFAULT '{"text": "你好世界🌍"}',
    escaped JSON DEFAULT '{"text": "line1\\nline2\\ttab"}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- Verify: Insert data and query
INSERT INTO test_json_strict_edge_cases (id) VALUES (1);
SELECT * FROM test_json_strict_edge_cases;

-- ============================================
-- Test 10: More Invalid Formats - Should fail
-- ============================================

-- 10.1: Array with trailing comma
-- Expected: ERROR
CREATE TABLE test_json_strict_invalid_array_trailing_comma (
    id INT,
    data JSON DEFAULT '[1, 2, 3,]'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- 10.2: Multiple trailing commas
-- Expected: ERROR
CREATE TABLE test_json_strict_invalid_multi_trailing_comma (
    id INT,
    data JSON DEFAULT '{"a": 1,,}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- 10.3: Unquoted string value
-- Expected: ERROR
CREATE TABLE test_json_strict_invalid_unquoted_value (
    id INT,
    data JSON DEFAULT '{"key": value}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- 10.4: NaN (non-standard JSON)
-- Expected: ERROR
CREATE TABLE test_json_strict_invalid_nan (
    id INT,
    data JSON DEFAULT '{"value": NaN}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- 10.5: Infinity (non-standard JSON)
-- Expected: ERROR
CREATE TABLE test_json_strict_invalid_infinity (
    id INT,
    data JSON DEFAULT '{"value": Infinity}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

