-- name: test_variant_null_count_and_json
-- A variant null (the value of '$.a' in {"a": null}) is not a SQL NULL: `v IS NULL` is false, so count(v) has to count
-- it. count(VARIANT) used to count cast(v as VARCHAR), which is a SQL NULL for a variant null.
SELECT count(v), count(*), sum(if(v IS NULL, 1, 0))
FROM (SELECT variant_query(parse_json('{"a": null}'), '$.a') AS v
      UNION ALL SELECT variant_query(parse_json('{"a": 1}'), '$.a')
      UNION ALL SELECT CAST(NULL AS VARIANT)) t;
SELECT count(variant_query(parse_json('{"a": null}'), '$.a')), count(variant_query(parse_json('{"a": 1}'), '$.b'));
SELECT k, count(v) FROM (
    SELECT 1 AS k, variant_query(parse_json('{"a": null}'), '$.a') AS v
    UNION ALL SELECT 1, variant_query(parse_json('{"a": 2}'), '$.a')
    UNION ALL SELECT 2, CAST(NULL AS VARIANT)) t
GROUP BY k ORDER BY k;

-- CAST(VARIANT AS JSON) keeps a scalar's JSON type instead of turning it into a JSON string; json_array shows the
-- difference between 1 and "1". A variant null casts to SQL NULL, as it does for the other cast targets.
SELECT json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.a') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.b') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.c') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.d') AS JSON));
SELECT CAST(variant_query(parse_json('{"a": null}'), '$.a') AS JSON) IS NULL,
       CAST(variant_query(parse_json('{"a": null}'), '$.a') AS STRING) IS NULL;
SELECT CAST(CAST(parse_json('{"a": null, "b": [1, null]}') AS VARIANT) AS JSON);
SELECT CAST(variant_query(parse_json('{"a": 1}'), '$.a') AS JSON) = CAST(1 AS JSON);

-- The cast keeps a DOUBLE exact: 0.3 must not come back as the next double.
SELECT CAST(CAST(CAST(0.3 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(0.3 AS DOUBLE) AS JSON),
       CAST(CAST(CAST(-0.3 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(-0.3 AS DOUBLE) AS JSON),
       CAST(CAST(CAST(0.7 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(0.7 AS DOUBLE) AS JSON);

-- count distinct has no VARIANT form and keeps counting the VARCHAR text of the values in every aggregation stage.
SELECT /*+ SET_VAR(new_planner_agg_stage=2) */ count(DISTINCT CAST(x AS VARIANT))
FROM (SELECT generate_series % 7 AS x FROM TABLE(generate_series(1, 1000))) t;
SELECT /*+ SET_VAR(new_planner_agg_stage=3) */ count(DISTINCT CAST(x AS VARIANT))
FROM (SELECT generate_series % 7 AS x FROM TABLE(generate_series(1, 1000))) t;
