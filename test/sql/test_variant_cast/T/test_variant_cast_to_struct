-- name: test_variant_cast_to_struct
-- Field names that are not simple path keys are literal object keys (#74122).
SELECT CAST(CAST(PARSE_JSON('{"$currency": "USD"}') AS VARIANT) AS STRUCT<`$currency` STRING>);
SELECT CAST(CAST(PARSE_JSON('{"First Name": "Kyle"}') AS VARIANT) AS STRUCT<`First Name` STRING>);
SELECT CAST(CAST(PARSE_JSON('{"a.b": "v"}') AS VARIANT) AS STRUCT<`a.b` STRING>);
-- A dotted name is one key, never a nested path.
SELECT CAST(CAST(PARSE_JSON('{"a": {"b": "x"}}') AS VARIANT) AS STRUCT<`a.b` STRING>);
-- A bracketed name is one key, never an array index.
SELECT CAST(CAST(PARSE_JSON('{"a[0]": "v", "a": ["wrong"]}') AS VARIANT) AS STRUCT<`a[0]` STRING>);
SELECT CAST(CAST(PARSE_JSON('{"inner": {"$x": 5}}') AS VARIANT) AS STRUCT<`inner` STRUCT<`$x` INT>>);
SELECT CAST(CAST(PARSE_JSON('{"id": 1, "$currency": "USD"}') AS VARIANT) AS STRUCT<id INT, `$currency` STRING, `not exist` STRING>);
-- A non-object root yields a NULL row, not a struct of NULL fields, even for a non-nullable input.
SELECT CAST(CAST(PARSE_JSON('[1, 2]') AS VARIANT) AS STRUCT<x INT>) IS NULL;
SELECT CAST(CAST(PARSE_JSON('5') AS VARIANT) AS STRUCT<x INT>) IS NULL;
create database test_variant_cast_to_struct_${uuid0};
use test_variant_cast_to_struct_${uuid0};
create table t_with_null (id int, j json) duplicate key(id) distributed by hash(id) buckets 1 properties ("replication_num" = "1");
insert into t_with_null values (1, parse_json('{"$currency": "USD", "First Name": "Kyle"}')), (2, parse_json('{"$currency": "EUR"}')), (3, parse_json('[1, 2]')), (4, null);
create table t_no_null (id int, j json) duplicate key(id) distributed by hash(id) buckets 1 properties ("replication_num" = "1");
insert into t_no_null values (1, parse_json('{"x": 1}')), (2, parse_json('[1, 2]')), (3, parse_json('5')), (4, parse_json('{"y": 1}'));
[ORDER] SELECT id, CAST(CAST(j AS VARIANT) AS STRUCT<`$currency` STRING, `First Name` STRING>) FROM t_with_null ORDER BY id;
-- No NULL in the chunk, so CAST(j AS VARIANT) is non-nullable; the non-object rows must still be NULL.
[ORDER] SELECT id, CAST(CAST(j AS VARIANT) AS STRUCT<x INT>) AS s, CAST(CAST(j AS VARIANT) AS STRUCT<x INT>) IS NULL AS is_null FROM t_no_null ORDER BY id;
drop database test_variant_cast_to_struct_${uuid0};
