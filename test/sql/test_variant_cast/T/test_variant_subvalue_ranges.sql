-- name: test_variant_subvalue_ranges

WITH src AS (
    SELECT CAST(named_struct('a', 7, 'b', 'tail', 'nested', named_struct('k', 42)) AS VARIANT) AS v
)
SELECT get_variant_int(v, '$.a'), get_variant_string(v, '$.b'), get_variant_int(v, '$.nested.k')
FROM src;

WITH src AS (
    SELECT CAST([named_struct('x', 1, 's', 'first'), named_struct('x', 2, 's', 'second')] AS VARIANT) AS v
), decoded AS (
    SELECT CAST(v AS ARRAY<VARIANT>) AS a FROM src
)
SELECT array_length(a), get_variant_int(a[1], '$.x'), get_variant_string(a[2], '$.s')
FROM decoded;

WITH src AS (
    SELECT CAST(array_agg(generate_series ORDER BY generate_series) AS VARIANT) AS v
    FROM TABLE(generate_series(1, 128))
), decoded AS (
    SELECT CAST(v AS ARRAY<VARIANT>) AS a FROM src
)
SELECT array_length(a), CAST(a[1] AS BIGINT), CAST(a[128] AS BIGINT),
       array_sum(array_map(x -> CAST(x AS BIGINT), a))
FROM decoded;

WITH src AS (
    SELECT CAST(named_struct('a', repeat('x', 200), 'z', 9) AS VARIANT) AS v
)
SELECT length(get_variant_string(v, '$.a')), get_variant_int(v, '$.z')
FROM src;

WITH src AS (
    SELECT CAST(named_struct('a', CAST(NULL AS INT), 'b', [1, 2, 3], 'c', named_struct('k', 'end')) AS VARIANT) AS v
)
SELECT COALESCE(CAST(get_variant_int(v, '$.a') AS VARCHAR), 'SQL_NULL'),
       array_length(CAST(variant_query(v, '$.b') AS ARRAY<VARIANT>)),
       get_variant_string(v, '$.c.k')
FROM src;
