-- name: test_get_variant_complex
SELECT
    CASE WHEN CAST(CAST(ARRAY<int>[1,2,3] AS VARIANT) AS ARRAY<INT>)[1] = 1 THEN 'PASS' ELSE 'FAIL' END AS v_array_elem1,
    CASE WHEN CAST(CAST(ARRAY<int>[1,2,3] AS VARIANT) AS ARRAY<INT>)[3] = 3 THEN 'PASS' ELSE 'FAIL' END AS v_array_elem3,
    CASE WHEN array_length(CAST(CAST(ARRAY<int>[1,2,3] AS VARIANT) AS ARRAY<INT>)) = 3 THEN 'PASS' ELSE 'FAIL' END AS v_array_len;
-- result:
PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(MAP{'a':1,'b':2} AS VARIANT) AS MAP<VARCHAR,INT>)['a'] = 1 THEN 'PASS' ELSE 'FAIL' END AS v_map_a,
    CASE WHEN CAST(CAST(MAP{'a':1,'b':2} AS VARIANT) AS MAP<VARCHAR,INT>)['b'] = 2 THEN 'PASS' ELSE 'FAIL' END AS v_map_b;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(named_struct('a', 1, 'b', 2) AS VARIANT) AS STRUCT<a INT, b INT>).a = 1 THEN 'PASS' ELSE 'FAIL' END AS v_struct_a,
    CASE WHEN CAST(CAST(named_struct('a', 1, 'b', 2) AS VARIANT) AS STRUCT<a INT, b INT>).b = 2 THEN 'PASS' ELSE 'FAIL' END AS v_struct_b;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(STRUCT(ARRAY<int>[1,2], MAP{'k':1}) AS VARIANT) AS STRUCT<col1 ARRAY<INT>, col2 MAP<VARCHAR,INT>>).col1[2] = 2
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_array,
    CASE WHEN CAST(CAST(STRUCT(ARRAY<int>[1,2], MAP{'k':1}) AS VARIANT) AS STRUCT<col1 ARRAY<INT>, col2 MAP<VARCHAR,INT>>).col2['k'] = 1
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_map;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(MAP{'x':ARRAY<int>[1,2], 'y':ARRAY<int>[] } AS VARIANT) AS MAP<VARCHAR,ARRAY<INT>>)['x'][2] = 2
        THEN 'PASS' ELSE 'FAIL' END AS v_map_array_x,
    CASE WHEN array_length(CAST(CAST(MAP{'x':ARRAY<int>[1,2], 'y':ARRAY<int>[] } AS VARIANT) AS MAP<VARCHAR,ARRAY<INT>>)['y']) = 0
        THEN 'PASS' ELSE 'FAIL' END AS v_map_array_y;
-- result:
PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<STRUCT<col1 int,col2 varchar>>[STRUCT(1,'a'),STRUCT(2,'b')] AS VARIANT)
        AS ARRAY<STRUCT<col1 int,col2 varchar>>)[2].col2 = 'b'
        THEN 'PASS' ELSE 'FAIL' END AS v_array_struct_elem;
-- result:
PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(ARRAY<int>[1,2] AS VARIANT) AS ARRAY<VARIANT>)[1] IS NOT NULL
        THEN 'PASS' ELSE 'FAIL' END AS v_array_variant,
    CASE WHEN CAST(CAST(MAP{'a':1,'b':2} AS VARIANT) AS MAP<VARCHAR,VARIANT>)['a'] IS NOT NULL
        THEN 'PASS' ELSE 'FAIL' END AS v_map_variant,
    CASE WHEN CAST(CAST(named_struct('a', 1, 'b', 2) AS VARIANT) AS STRUCT<a INT>).a = 1
        THEN 'PASS' ELSE 'FAIL' END AS v_struct_subset;
-- result:
PASS	PASS	PASS
-- !result
SELECT
    CASE WHEN CAST(CAST(CAST(ARRAY<int>[1,2,3] AS VARIANT) AS JSON) AS VARCHAR) = '[1, 2, 3]'
        THEN 'PASS' ELSE 'FAIL' END AS v_variant_json_array,
    CASE WHEN CAST(CAST(CAST(MAP{'a':1} AS VARIANT) AS JSON) AS VARCHAR) = '{"a": 1}'
        THEN 'PASS' ELSE 'FAIL' END AS v_variant_json_map,
    CASE WHEN CAST(CAST(CAST(named_struct('a', 1, 'b', 2) AS VARIANT) AS JSON) AS VARCHAR) = '{"a": 1, "b": 2}'
        THEN 'PASS' ELSE 'FAIL' END AS v_variant_json_struct;
-- result:
PASS	PASS	PASS
-- !result