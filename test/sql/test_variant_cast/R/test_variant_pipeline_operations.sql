-- name: test_variant_pipeline_operations

SELECT SUM(array_length(w))
FROM (
    SELECT array_agg(CAST(generate_series AS VARIANT)) OVER (
        ORDER BY generate_series ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS w
    FROM TABLE(generate_series(1, 10000))
) t;
-- result:
19999
-- !result

[ORDER] SELECT k, CAST(v AS VARCHAR) AS v
FROM (
    SELECT
        generate_series AS k,
        CASE WHEN generate_series = 3
            THEN NULL
            ELSE CAST(generate_series AS VARIANT)
        END AS v
    FROM TABLE(generate_series(1, 4))
) t
ORDER BY k
LIMIT 2 OFFSET 1;
-- result:
2	2
3	None
-- !result

[ORDER] SELECT
    generate_series,
    CASE WHEN CAST(generate_series AS VARIANT) IN (CAST(1 AS VARIANT), CAST(2 AS VARIANT))
        THEN 'IN' ELSE 'OUT' END AS in_result,
    CAST(CASE
        WHEN generate_series = 1 THEN CAST(named_struct('a', 1) AS VARIANT)
        WHEN generate_series = 2 THEN CAST(named_struct('b', 2) AS VARIANT)
        ELSE CAST(named_struct('c', 3) AS VARIANT)
    END AS VARCHAR) AS searched_case_result,
    CAST(CASE generate_series
        WHEN 1 THEN CAST(10 AS VARIANT)
        WHEN 2 THEN CAST(20 AS VARIANT)
        ELSE CAST(30 AS VARIANT)
    END AS VARCHAR) AS simple_case_result
FROM TABLE(generate_series(1, 3))
ORDER BY generate_series;
-- result:
1	IN	{"a":1}	10
2	IN	{"b":2}	20
3	OUT	{"c":3}	30
-- !result

[ORDER] SELECT
    generate_series,
    CASE WHEN array_length(repeated) = 2
              AND CAST(repeated[2] AS VARCHAR) = CAST(generate_series AS VARCHAR)
        THEN 'PASS' ELSE 'FAIL' END AS repeat_result,
    CASE WHEN array_length(sliced) = 2
              AND CAST(sliced[1] AS VARCHAR) = CAST(generate_series + 10 AS VARCHAR)
              AND sliced[2] IS NULL
        THEN 'PASS' ELSE 'FAIL' END AS slice_result,
    CASE WHEN array_length(flattened) = 3
              AND flattened[2] IS NULL
              AND CAST(flattened[3] AS VARCHAR) = CAST(generate_series + 1 AS VARCHAR)
        THEN 'PASS' ELSE 'FAIL' END AS flatten_result
FROM (
    SELECT
        generate_series,
        array_repeat(CAST(generate_series AS VARIANT), 2) AS repeated,
        array_slice(ARRAY<VARIANT>[
            CAST(generate_series AS VARIANT),
            CAST(generate_series + 10 AS VARIANT),
            CAST(NULL AS VARIANT)
        ], 2, 2) AS sliced,
        array_flatten(ARRAY<ARRAY<VARIANT>>[
            ARRAY<VARIANT>[CAST(generate_series AS VARIANT), CAST(NULL AS VARIANT)],
            ARRAY<VARIANT>[CAST(generate_series + 1 AS VARIANT)]
        ]) AS flattened
    FROM TABLE(generate_series(1, 2))
) t
ORDER BY generate_series;
-- result:
1	PASS	PASS	PASS
2	PASS	PASS	PASS
-- !result

SELECT
    CASE WHEN CAST(CAST(ARRAY<VARIANT>[
            CAST(named_struct('a', 1) AS VARIANT),
            CAST(named_struct('b', 2) AS VARIANT),
            CAST(NULL AS VARIANT)
        ] AS VARIANT) AS VARCHAR) = '[{"a":1},{"b":2},null]'
        THEN 'PASS' ELSE 'FAIL' END AS nested_array_result,
    CASE WHEN CAST(CAST(MAP{
            'left': CAST(named_struct('a', 1) AS VARIANT),
            'right': CAST(named_struct('b', 2) AS VARIANT)
        } AS VARIANT) AS VARCHAR) = '{"left":{"a":1},"right":{"b":2}}'
        THEN 'PASS' ELSE 'FAIL' END AS nested_map_result,
    CASE WHEN CAST(CAST(named_struct(
            'left', CAST(named_struct('a', 1) AS VARIANT),
            'right', CAST(ARRAY<int>[1, 2] AS VARIANT)
        ) AS VARIANT) AS VARCHAR) = '{"left":{"a":1},"right":[1,2]}'
        THEN 'PASS' ELSE 'FAIL' END AS nested_struct_result;
-- result:
PASS	PASS	PASS
-- !result
