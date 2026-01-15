-- name: test_cast_to_variant_complex
SELECT
    CASE WHEN CAST(CAST(ARRAY<int>[1,2,3] AS VARIANT) AS VARCHAR) = '[1,2,3]'
        THEN 'PASS' ELSE 'FAIL' END AS v_array_roundtrip,
    CASE WHEN CAST(CAST(MAP{'b':2,'a':1} AS VARIANT) AS VARCHAR) = '{"a":1,"b":2}'
        THEN 'PASS' ELSE 'FAIL' END AS v_map_roundtrip,
    CASE WHEN CAST(CAST(STRUCT(1, 'x') AS VARIANT) AS VARCHAR) = '{"col1":1,"col2":"x"}'
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_roundtrip;
-- result:
PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<int>[1,NULL,3] AS VARIANT) AS VARCHAR) = '[1,null,3]'
        THEN 'PASS' ELSE 'FAIL' END AS v_array_null_element,
    CASE WHEN CAST(CAST(MAP{'a':NULL,'b':2} AS VARIANT) AS VARCHAR) = '{"a":null,"b":2}'
        THEN 'PASS' ELSE 'FAIL' END AS v_map_null_value,
    CASE WHEN CAST(CAST(STRUCT(NULL, 'x') AS VARIANT) AS VARCHAR) = '{"col1":null,"col2":"x"}'
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_null_field;
-- result:
PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<int>[] AS VARIANT) AS VARCHAR) = '[]'
        THEN 'PASS' ELSE 'FAIL' END AS v_array_empty,
    CASE WHEN CAST(CAST(MAP{} AS VARIANT) AS VARCHAR) = '{}'
        THEN 'PASS' ELSE 'FAIL' END AS v_map_empty;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(STRUCT(NULL) AS VARIANT) AS VARCHAR) = '{"col1":null}'
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_single_null;
-- result:
PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<STRUCT<col1 int,col2 varchar>>[STRUCT(1,'a'),STRUCT(2,'b')] AS VARIANT) AS VARCHAR) = '[{"col1":1,"col2":"a"},{"col1":2,"col2":"b"}]'
        THEN 'PASS' ELSE 'FAIL' END AS v_array_struct_nested,
    CASE WHEN CAST(CAST(MAP{'x':ARRAY<int>[1,2], 'y':ARRAY<int>[] } AS VARIANT) AS VARCHAR)
        = '{"x":[1,2],"y":[]}' THEN 'PASS' ELSE 'FAIL' END AS v_map_array_nested;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<MAP<varchar,int>>[MAP{'a':1},MAP{'b':2}] AS VARIANT) AS VARCHAR)
        = '[{"a":1},{"b":2}]' THEN 'PASS' ELSE 'FAIL' END AS v_array_map_nested,
    CASE WHEN CAST(CAST(MAP{'k':STRUCT(1,'x')} AS VARIANT) AS VARCHAR)
        = '{"k":{"col1":1,"col2":"x"}}' THEN 'PASS' ELSE 'FAIL' END AS v_map_struct_nested,
    CASE WHEN CAST(CAST(STRUCT(ARRAY<int>[1,2], MAP{'a':1}) AS VARIANT) AS VARCHAR)
        = '{"col1":[1,2],"col2":{"a":1}}' THEN 'PASS' ELSE 'FAIL' END AS v_struct_mixed_nested,
    CASE WHEN CAST(CAST(MAP{1:'a',2:'b'} AS VARIANT) AS VARCHAR)
        = '{"1":"a","2":"b"}' THEN 'PASS' ELSE 'FAIL' END AS v_map_int_key_cast;
-- result:
PASS	PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(STRUCT(MAP{'a':ARRAY<STRUCT<col1 int,col2 varchar>>[STRUCT(1,'x'),STRUCT(NULL,'y')]},
                                    ARRAY<MAP<varchar,int>>[MAP{'k':1},MAP{}]) AS VARIANT) AS VARCHAR)
        = '{"col1":{"a":[{"col1":1,"col2":"x"},{"col1":null,"col2":"y"}]},"col2":[{"k":1},{}]}'
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_deep_nested_nulls;
-- result:
PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(MAP{1:STRUCT(ARRAY<MAP<varchar,int>>[MAP{'a':1},MAP{'b':2}])} AS VARIANT) AS VARCHAR) = '{"1":{"col1":[{"a":1},{"b":2}]}}'
        THEN 'PASS' ELSE 'FAIL' END AS v_map_struct_array_map;
-- result:
PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(named_struct('a', 1, 'b', 2, 'c', 3) AS VARIANT) AS VARCHAR) = '{"a":1,"b":2,"c":3}'
        THEN 'PASS' ELSE 'FAIL' END AS v_named_struct_roundtrip;
-- result:
PASS
-- !result