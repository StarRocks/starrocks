-- name: test_variant_typeof_only_null
SELECT variant_typeof(variant_query(s.v, '$.age')) FROM (SELECT CAST('age' AS VARIANT) AS v) s;
-- result:
None
-- !result
SELECT variant_typeof(variant_query(s.v, '$.missing')) FROM (SELECT PARSE_JSON('{"age": 7}') AS v) s;
-- result:
Null
-- !result
SELECT variant_typeof(variant_query(s.v, '$.age')) FROM (SELECT PARSE_JSON('{"age": 7}') AS v) s;
-- result:
Int8
-- !result
SELECT variant_typeof(variant_query(s.v, '$.name')) FROM (SELECT PARSE_JSON('{"name": "x"}') AS v) s;
-- result:
String
-- !result
SELECT variant_typeof(s.v) FROM (SELECT CAST('age' AS VARIANT) AS v) s;
-- result:
String
-- !result
SELECT variant_typeof(CAST(NULL AS VARIANT));
-- result:
None
-- !result