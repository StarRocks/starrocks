-- name: test_variant_null_count_and_json
SELECT count(v), count(*), sum(if(v IS NULL, 1, 0))
FROM (SELECT variant_query(parse_json('{"a": null}'), '$.a') AS v
      UNION ALL SELECT variant_query(parse_json('{"a": 1}'), '$.a')
      UNION ALL SELECT CAST(NULL AS VARIANT)) t;
-- result:
2	3	1
-- !result
SELECT count(variant_query(parse_json('{"a": null}'), '$.a')), count(variant_query(parse_json('{"a": 1}'), '$.b'));
-- result:
1	0
-- !result
SELECT k, count(v) FROM (
    SELECT 1 AS k, variant_query(parse_json('{"a": null}'), '$.a') AS v
    UNION ALL SELECT 1, variant_query(parse_json('{"a": 2}'), '$.a')
    UNION ALL SELECT 2, CAST(NULL AS VARIANT)) t
GROUP BY k ORDER BY k;
-- result:
1	2
2	0
-- !result
SELECT json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.a') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.b') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.c') AS JSON)),
       json_array(CAST(variant_query(parse_json('{"a": 1, "b": true, "c": 1.5, "d": "s"}'), '$.d') AS JSON));
-- result:
[1]	[true]	[1.5]	["s"]
-- !result
SELECT CAST(variant_query(parse_json('{"a": null}'), '$.a') AS JSON) IS NULL,
       CAST(variant_query(parse_json('{"a": null}'), '$.a') AS STRING) IS NULL;
-- result:
1	1
-- !result
SELECT CAST(CAST(parse_json('{"a": null, "b": [1, null]}') AS VARIANT) AS JSON);
-- result:
{"a": null, "b": [1, null]}
-- !result
SELECT CAST(variant_query(parse_json('{"a": 1}'), '$.a') AS JSON) = CAST(1 AS JSON);
-- result:
1
-- !result
SELECT CAST(CAST(CAST(0.3 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(0.3 AS DOUBLE) AS JSON),
       CAST(CAST(CAST(-0.3 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(-0.3 AS DOUBLE) AS JSON),
       CAST(CAST(CAST(0.7 AS DOUBLE) AS VARIANT) AS JSON) = CAST(CAST(0.7 AS DOUBLE) AS JSON);
-- result:
1	1	1
-- !result
SELECT /*+ SET_VAR(new_planner_agg_stage=2) */ count(DISTINCT CAST(x AS VARIANT))
FROM (SELECT generate_series % 7 AS x FROM TABLE(generate_series(1, 1000))) t;
-- result:
7
-- !result
SELECT /*+ SET_VAR(new_planner_agg_stage=3) */ count(DISTINCT CAST(x AS VARIANT))
FROM (SELECT generate_series % 7 AS x FROM TABLE(generate_series(1, 1000))) t;
-- result:
7
-- !result