-- name: test_cast_to_variant_complex
assert_query_contains("SELECT CAST(ARRAY<int>[1,2,3] AS VARIANT)", "not supported yet");
-- result:
E: (1064, "Getting syntax error at line 1, column 0. Detail message: Unexpected input 'assert_query_contains', the most similar input is {'RESTORE', 'SET', 'INSERT', 'CREATE', 'RESUME', '(', ';'}.")
-- !result
assert_query_contains("SELECT CAST(MAP{'a':1} AS VARIANT)", "not supported yet");
-- result:
E: (1064, "Getting syntax error at line 1, column 0. Detail message: Unexpected input 'assert_query_contains', the most similar input is {'RESTORE', 'SET', 'INSERT', 'CREATE', 'RESUME', '(', ';'}.")
-- !result
assert_query_contains("SELECT CAST(STRUCT(1, 'x') AS VARIANT)", "not supported yet");
-- result:
E: (1064, "Getting syntax error at line 1, column 0. Detail message: Unexpected input 'assert_query_contains', the most similar input is {'RESTORE', 'SET', 'INSERT', 'CREATE', 'RESUME', '(', ';'}.")
-- !result