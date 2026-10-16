-- name: test_variant_typeof_only_null
-- variant_typeof down_cast'd its argument to VariantColumn without the only-null guard every other
-- function in the file has. An only-null column carries no type to unwrap -- get_data_column takes
-- it down to the placeholder create_const_null_column builds -- so the cast asserted and took the
-- BE down. variant_query returns exactly such a column when the path is not present, and a scalar
-- variant has no paths at all.

-- the statement the fuzzer crashed on, reduced: '$.age' is absent from a scalar variant
SELECT variant_typeof(variant_query(s.v, '$.age')) FROM (SELECT CAST('age' AS VARIANT) AS v) s;

-- absent from an object variant reaches the same only-null column
SELECT variant_typeof(variant_query(s.v, '$.missing')) FROM (SELECT PARSE_JSON('{"age": 7}') AS v) s;

-- present: the answer has to be the field's own type, so this pins behaviour and not just survival
SELECT variant_typeof(variant_query(s.v, '$.age')) FROM (SELECT PARSE_JSON('{"age": 7}') AS v) s;
SELECT variant_typeof(variant_query(s.v, '$.name')) FROM (SELECT PARSE_JSON('{"name": "x"}') AS v) s;

-- the argument reaching variant_typeof directly was never broken; kept as the control
SELECT variant_typeof(s.v) FROM (SELECT CAST('age' AS VARIANT) AS v) s;
SELECT variant_typeof(CAST(NULL AS VARIANT));
