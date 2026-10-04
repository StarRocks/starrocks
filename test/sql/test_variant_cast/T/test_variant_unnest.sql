-- name: test_variant_unnest

[ORDER] SELECT CAST(u.unnest AS VARCHAR) AS v
FROM (SELECT CAST([1, 2] AS ARRAY<VARIANT>) AS a) src, UNNEST(src.a) u
ORDER BY v;

[ORDER] WITH src AS (
    SELECT 1 AS id, ARRAY<VARIANT>[CAST(10 AS VARIANT), CAST(NULL AS VARIANT)] AS a
    UNION ALL SELECT 2, CAST(NULL AS ARRAY<VARIANT>)
    UNION ALL SELECT 3, ARRAY<VARIANT>[]
    UNION ALL SELECT 4, ARRAY<VARIANT>[CAST(named_struct('k', 20) AS VARIANT)]
)
SELECT src.id, COALESCE(CAST(u.unnest AS VARCHAR), 'SQL_NULL') AS v
FROM src, UNNEST(src.a) u
ORDER BY src.id, v;

[ORDER] WITH src AS (
    SELECT 1 AS id, ARRAY<VARIANT>[CAST(10 AS VARIANT), CAST(NULL AS VARIANT)] AS a
    UNION ALL SELECT 2, CAST(NULL AS ARRAY<VARIANT>)
    UNION ALL SELECT 3, ARRAY<VARIANT>[]
    UNION ALL SELECT 4, ARRAY<VARIANT>[CAST(named_struct('k', 20) AS VARIANT)]
)
SELECT src.id, COALESCE(CAST(u.unnest AS VARCHAR), 'SQL_NULL') AS v
FROM src LEFT JOIN UNNEST(src.a) u ON TRUE
ORDER BY src.id, v;

WITH src AS (
    SELECT CAST([1, 2] AS ARRAY<VARIANT>) AS a
    UNION ALL SELECT CAST(NULL AS ARRAY<VARIANT>)
    UNION ALL SELECT ARRAY<VARIANT>[]
)
SELECT COUNT(*) FROM src, UNNEST(src.a) u;

WITH src AS (
    SELECT ARRAY<ARRAY<VARIANT>>[
        ARRAY<VARIANT>[CAST(1 AS VARIANT), CAST(NULL AS VARIANT)],
        ARRAY<VARIANT>[CAST(2 AS VARIANT), CAST(3 AS VARIANT)],
        ARRAY<VARIANT>[]
    ] AS a
)
SELECT COUNT(*), SUM(CAST(v.unnest AS BIGINT))
FROM src, UNNEST(src.a) a, UNNEST(a.unnest) v;

-- Exercise two-level Variant path extraction and lateral expansion.
[ORDER] WITH src AS (
    SELECT CAST(named_struct('cloud_physical', named_struct('cameras', [
        named_struct('item_results', [
            named_struct('item_key', 'physical.frame_continuity', 'status', 'passed'),
            named_struct('item_key', 'other', 'status', 'failed')
        ]),
        named_struct('item_results', [
            named_struct('item_key', 'physical.frame_continuity', 'status', 'failed'),
            named_struct('item_key', 'physical.frame_continuity', 'status', 'passed')
        ])
    ])) AS VARIANT) AS quality_summary
)
SELECT get_variant_string(item_entry.unnest, '$.status') AS status, COUNT(*) AS item_rows
FROM src,
    UNNEST(CAST(variant_query(src.quality_summary, '$.cloud_physical.cameras') AS ARRAY<VARIANT>)) camera_entry,
    UNNEST(CAST(variant_query(camera_entry.unnest, '$.item_results') AS ARRAY<VARIANT>)) item_entry
WHERE get_variant_string(item_entry.unnest, '$.item_key') = 'physical.frame_continuity'
GROUP BY status
ORDER BY status;
