-- name: test_json_strict_validation
DROP DATABASE IF EXISTS test_json_strict_db;
-- result:
-- !result
CREATE DATABASE test_json_strict_db;
-- result:
-- !result
USE test_json_strict_db;
-- result:
-- !result
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
-- result:
-- !result
INSERT INTO test_json_strict_valid (id) VALUES (1);
-- result:
-- !result
SELECT * FROM test_json_strict_valid;
-- result:
1	{"key": "value"}	{"age": 30, "name": "Alice"}	{}	"null"	""	"123"	"true"
-- !result
CREATE TABLE test_json_strict_invalid_unquoted_key (
    id INT,
    data JSON DEFAULT '{key: "value"}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {key: "value"}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 3 path $..')
-- !result
CREATE TABLE test_json_strict_invalid_trailing_comma (
    id INT,
    data JSON DEFAULT '{"key": "value",}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"key": "value",}. Error: com.google.gson.stream.MalformedJsonException: Expected name at line 1 column 18 path $.key.')
-- !result
CREATE TABLE test_json_strict_invalid_single_quote (
    id INT,
    data JSON DEFAULT "{'key': 'value'}"
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, "Getting analyzing error. Detail message: Invalid default value for 'data': Invalid JSON format for default value: {'key': 'value'}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 3 path $..")
-- !result
CREATE TABLE test_json_strict_invalid_comment (
    id INT,
    data JSON DEFAULT '{"key": "value" /* comment */}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"key": "value" /* comment */}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 18 path $.key.')
-- !result
CREATE TABLE test_json_strict_valid_complex (
    id INT,
    data JSON DEFAULT '{"user": {"name": "Bob", "age": 25, "address": {"city": "Beijing", "zip": "100000"}}, "tags": ["a", "b", "c"], "active": true, "score": 95.5}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_json_strict_valid_complex (id) VALUES (1);
-- result:
-- !result
SELECT * FROM test_json_strict_valid_complex;
-- result:
1	{"active": true, "score": 95.5, "tags": ["a", "b", "c"], "user": {"address": {"city": "Beijing", "zip": "100000"}, "age": 25, "name": "Bob"}}
-- !result
CREATE TABLE test_json_strict_valid_nested (
    id INT,
    data JSON DEFAULT '{"level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}}}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_json_strict_valid_nested (id) VALUES (1);
-- result:
-- !result
SELECT * FROM test_json_strict_valid_nested;
-- result:
1	{"level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}}}
-- !result
CREATE TABLE test_json_strict_valid_array (
    id INT,
    data1 JSON DEFAULT '[1, 2, 3, 4, 5]',
    data2 JSON DEFAULT '[{"name": "Alice"}, {"name": "Bob"}]',
    data3 JSON DEFAULT '[[1, 2], [3, 4], [5, 6]]',
    data4 JSON DEFAULT '[]'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
-- !result
INSERT INTO test_json_strict_valid_array (id) VALUES (1);
-- result:
-- !result
SELECT * FROM test_json_strict_valid_array;
-- result:
1	[1, 2, 3, 4, 5]	[{"name": "Alice"}, {"name": "Bob"}]	[[1, 2], [3, 4], [5, 6]]	[]
-- !result
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
-- result:
-- !result
INSERT INTO test_json_strict_edge_cases (id) VALUES (1);
-- result:
-- !result
SELECT * FROM test_json_strict_edge_cases;
-- result:
1	{}	[]	"null"	""	"0"	"-123.45"	"true"	"false"	{"text": "你好世界🌍"}	{"text": "line1\nline2\ttab"}
-- !result
CREATE TABLE test_json_strict_invalid_array_trailing_comma (
    id INT,
    data JSON DEFAULT '[1, 2, 3,]'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, "Getting analyzing error. Detail message: Invalid default value for 'data': Invalid JSON format for default value: [1, 2, 3,]. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 11 path $[3].")
-- !result
CREATE TABLE test_json_strict_invalid_multi_trailing_comma (
    id INT,
    data JSON DEFAULT '{"a": 1,,}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"a": 1,,}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 10 path $.a.')
-- !result
CREATE TABLE test_json_strict_invalid_unquoted_value (
    id INT,
    data JSON DEFAULT '{"key": value}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"key": value}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 9 path $.key.')
-- !result
CREATE TABLE test_json_strict_invalid_nan (
    id INT,
    data JSON DEFAULT '{"value": NaN}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"value": NaN}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 11 path $.value.')
-- !result
CREATE TABLE test_json_strict_invalid_infinity (
    id INT,
    data JSON DEFAULT '{"value": Infinity}'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Invalid default value for \'data\': Invalid JSON format for default value: {"value": Infinity}. Error: com.google.gson.stream.MalformedJsonException: Use JsonReader.setLenient(true) to accept malformed JSON at line 1 column 11 path $.value.')
-- !result