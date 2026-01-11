-- name: test_cast_to_variant_complex

SELECT CAST(ARRAY<int>[1,2,3] AS VARIANT);

SELECT CAST(MAP{'a':1} AS VARIANT);

SELECT CAST(STRUCT(1, 'x') AS VARIANT);