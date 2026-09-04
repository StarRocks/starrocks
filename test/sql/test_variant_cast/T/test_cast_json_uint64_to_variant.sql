-- name: test_cast_json_uint64_to_variant
-- The VARIANT encoder's vpack UInt branch kept a value up to INT64_MAX as an INT64 and sent every
-- larger one to a DOUBLE. A double carries a 53-bit mantissa, so the whole (INT64_MAX, UINT64_MAX]
-- range was rounded on the way in and two different uint64 values could encode to the same VARIANT.
-- Every other consumer of the same vpack UInt widens it to 128 bits instead: flat JSON stores such
-- a path as LARGEINT, the JSON column converter either keeps it as LARGEINT or reports an overflow,
-- and the LARGEINT -> VARIANT case in the encoder itself writes DECIMAL16 with scale 0. That is
-- what this branch writes now. Nothing at or below INT64_MAX moves, and a JSON integer above
-- UINT64_MAX is already a double before the encoder sees it, so no value that read back correctly
-- before this change reads back differently after it.
create database db_${uuid0};
use db_${uuid0};
create table t1_${uuid0} (k int, label varchar(32), j json, l largeint) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
-- k=1..3 and k=11 stay on the INT64 branch. k=4 is 2^63 and k=6 is 2^63+2048: both are above
-- INT64_MAX but a double holds them exactly, so they round-tripped before as well. k=5, k=7, k=8
-- and k=9 are the values a double loses. k=10 is above UINT64_MAX, which simdjson reports as a big
-- integer and json_value_converter turns into a double before the encoder is reached, so it is a
-- DOUBLE both before and after and is the boundary of what this change can repair.
insert into t1_${uuid0} values
  (1,  'int64_max_minus_1',  parse_json('{"v":9223372036854775806}'),  cast(9223372036854775806 as largeint)),
  (2,  'int64_max',          parse_json('{"v":9223372036854775807}'),  cast(9223372036854775807 as largeint)),
  (3,  'small',              parse_json('{"v":42}'),                   cast(42 as largeint)),
  (4,  'pow63',              parse_json('{"v":9223372036854775808}'),  cast(9223372036854775808 as largeint)),
  (5,  'int64_max_plus_2',   parse_json('{"v":9223372036854775809}'),  cast(9223372036854775809 as largeint)),
  (6,  'pow63_plus_2048',    parse_json('{"v":9223372036854777856}'),  cast(9223372036854777856 as largeint)),
  (7,  'uint64_max_minus_5', parse_json('{"v":18446744073709551610}'), cast(18446744073709551610 as largeint)),
  (8,  'uint64_max_minus_1', parse_json('{"v":18446744073709551614}'), cast(18446744073709551614 as largeint)),
  (9,  'uint64_max',         parse_json('{"v":18446744073709551615}'), cast(18446744073709551615 as largeint)),
  (10, 'uint64_max_plus_1',  parse_json('{"v":18446744073709551616}'), cast(18446744073709551616 as largeint)),
  (11, 'int64_min',          parse_json('{"v":-9223372036854775808}'), cast(-9223372036854775808 as largeint));
-- The JSON column and the LARGEINT column are the inputs and may not move.
select k, label, cast(j as varchar) as j_text, cast(l as varchar) as l_text from t1_${uuid0} order by k;
-- The encoder decision, one row at a time. Int64 for k=1..3 and k=11, Double for k=10, and
-- Decimal16 for the range this change repairs.
select k, label, variant_typeof(variant_query(cast(j as variant), '$.v')) as v_type
  from t1_${uuid0} order by k;
-- The value that comes back out. k=5, k=7, k=8 and k=9 were rounded before.
select k, label, cast(cast(j as variant) as varchar) as v_text from t1_${uuid0} order by k;
-- The LARGEINT -> VARIANT path already wrote DECIMAL16 and is the reference this change copies, so
-- every row here has to be byte for byte what it was.
select k, label, variant_typeof(cast(l as variant)) as l_type, cast(cast(l as variant) as varchar) as l_text
  from t1_${uuid0} order by k;
-- CAST(LARGEINT AS JSON) is the other producer of a vpack UInt in this range, so the
-- LARGEINT -> JSON -> VARIANT chain reaches the same branch. After the change it agrees with the
-- direct LARGEINT -> VARIANT cast on every row, which it did not before. k=10 is NULL because that
-- cast already rejects a LARGEINT above UINT64_MAX.
select k, label,
       cast(cast(cast(l as json) as variant) as varchar) as via_json,
       cast(cast(l as variant) as varchar) as direct
  from t1_${uuid0} order by k;
-- Two different uint64 values must not encode to the same VARIANT. Before the change k=4 and k=5
-- shared one encoding and k=7 through k=10 shared another, so eleven distinct JSON documents became
-- seven distinct VARIANTs. Every group here has to hold exactly one row.
select count(*) as rows_total,
       count(distinct cast(j as varchar)) as distinct_json,
       count(distinct cast(cast(j as variant) as varchar)) as distinct_variant
  from t1_${uuid0};
select cast(cast(j as variant) as varchar) as v_text, count(*) as rows_sharing_it
  from t1_${uuid0} group by v_text order by v_text;
select case when cast(a.j as variant) = cast(b.j as variant) then 'SAME' else 'DIFFERENT' end as variant_cmp,
       case when a.j = b.j then 'SAME' else 'DIFFERENT' end as json_cmp
  from t1_${uuid0} a, t1_${uuid0} b where a.k = 7 and b.k = 9;
-- The same collision reached through a filter instead of a projection, against a literal that the
-- planner folds on its own: before the change this predicate also matched k=8, k=9 and k=10.
select k, label from t1_${uuid0}
  where cast(j as variant) = cast(parse_json('{"v":18446744073709551610}') as variant) order by k;
-- Reading the repaired value back at a width that holds it.
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as largeint) as as_largeint,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,0)) as as_decimal_38_0,
       cast(variant_query(cast(j as variant), '$.v') as decimal(20,0)) as as_decimal_20_0
  from t1_${uuid0} order by k;
-- A target that cannot hold the value stays NULL rather than erroring, at both the integer and the
-- fractional end, and this change does not move that.
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as decimal(18,0)) as as_decimal_18_0,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,20)) as as_decimal_38_20,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,2)) as as_decimal_38_2
  from t1_${uuid0} order by k;
-- Reading the value as a DOUBLE applies the same rounding this branch used to apply at encode
-- time, so both DOUBLE columns hold on every row, including the repaired ones. Only the text
-- rendering moves, because it now has the digits to render.
select k, label,
       cast(cast(variant_query(cast(j as variant), '$.v') as double) as varchar) as as_double,
       cast(get_variant_double(cast(j as variant), '$.v') as varchar) as get_double,
       get_variant_string(cast(j as variant), '$.v') as get_string
  from t1_${uuid0} order by k;
-- Narrowing to BIGINT is out of range for every row above INT64_MAX and silently produces a
-- wrapped value both before and after this change: the DOUBLE branch produced the BIGINT minimum
-- and the DECIMAL16 branch produces the low 64 bits. Neither is the value and neither raises, and
-- deciding which one to report, or whether to raise instead, is deliberately left alone here.
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as bigint) as as_bigint,
       get_variant_int(cast(j as variant), '$.v') as get_int
  from t1_${uuid0} order by k;
-- The same vpack UInt reached through the three container entry points: a value inside an object,
-- an element of an array, and a JSON value carried through a MAP.
create table t2_${uuid0} (k int, j json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
insert into t2_${uuid0} values
  (1, parse_json('[18446744073709551615,1]')),
  (2, parse_json('{"a":{"b":18446744073709551615}}')),
  (3, parse_json('{"a":[9223372036854775809,18446744073709551615]}')),
  (4, parse_json('[9223372036854775807,42]'));
select k, cast(cast(j as variant) as varchar) as v_text from t2_${uuid0} order by k;
select k, cast(cast(map{'m': j} as variant) as varchar) as map_text from t2_${uuid0} order by k;
-- A JSON scalar extracted out of a document is a top-level vpack UInt when it reaches the encoder,
-- which is the same branch without a container around it.
select k, label, variant_typeof(cast(json_query(j, '$.v') as variant)) as scalar_type,
       cast(cast(json_query(j, '$.v') as variant) as varchar) as scalar_text
  from t1_${uuid0} order by k;
-- Flat JSON reads the same vpack UInt through its own extraction and already widened it to
-- LARGEINT, so none of these answers may move.
create table t3_${uuid0} (k int, j json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="true");
insert into t3_${uuid0} values
  (1, parse_json('{"v":9223372036854775807}')),
  (2, parse_json('{"v":9223372036854775809}')),
  (3, parse_json('{"v":18446744073709551615}')),
  (4, parse_json('{"v":18446744073709551616}'));
select k, cast(j->'$.v' as largeint) as flat_largeint,
       cast(cast(j->'$.v' as double) as varchar) as flat_double,
       cast(j as varchar) as j_text
  from t3_${uuid0} order by k;
select k, cast(cast(j as variant) as varchar) as v_text from t3_${uuid0} order by k;
