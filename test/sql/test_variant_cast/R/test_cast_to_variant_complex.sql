-- name: test_cast_to_variant_complex
SELECT CAST(ARRAY<int>[1,2,3] AS VARIANT);
-- result:
E: (1064, 'Getting analyzing error from line 1, column 7 to line 1, column 40. Detail message: CAST to VARIANT from array<int(11)> is not supported yet.')
-- !result
SELECT CAST(MAP{'a':1} AS VARIANT);
-- result:
E: (1064, 'Getting analyzing error from line 1, column 7 to line 1, column 33. Detail message: CAST to VARIANT from map<varchar,tinyint(4)> is not supported yet.')
-- !result
SELECT CAST(STRUCT(1, 'x') AS VARIANT);
-- result:
E: (1064, 'Getting analyzing error from line 1, column 7 to line 1, column 37. Detail message: CAST to VARIANT from struct<col1 tinyint(4), col2 varchar> is not supported yet.')
-- !result