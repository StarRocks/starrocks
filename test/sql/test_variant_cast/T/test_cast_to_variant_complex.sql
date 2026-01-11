-- name: test_cast_to_variant_complex

assert_query_contains("SELECT CAST(ARRAY<int>[1,2,3] AS VARIANT)", "not supported yet");
assert_query_contains("SELECT CAST(MAP{'a':1} AS VARIANT)", "not supported yet");
assert_query_contains("SELECT CAST(STRUCT(1, 'x') AS VARIANT)", "not supported yet");