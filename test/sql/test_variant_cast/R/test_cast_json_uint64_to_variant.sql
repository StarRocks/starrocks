-- name: test_cast_json_uint64_to_variant
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
create table t1_${uuid0} (k int, label varchar(32), j json, l largeint) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
-- result:
-- !result
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
-- result:
-- !result
select k, label, cast(j as varchar) as j_text, cast(l as varchar) as l_text from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	{"v": 9223372036854775806}	9223372036854775806
2	int64_max	{"v": 9223372036854775807}	9223372036854775807
3	small	{"v": 42}	42
4	pow63	{"v": 9223372036854775808}	9223372036854775808
5	int64_max_plus_2	{"v": 9223372036854775809}	9223372036854775809
6	pow63_plus_2048	{"v": 9223372036854777856}	9223372036854777856
7	uint64_max_minus_5	{"v": 18446744073709551610}	18446744073709551610
8	uint64_max_minus_1	{"v": 18446744073709551614}	18446744073709551614
9	uint64_max	{"v": 18446744073709551615}	18446744073709551615
10	uint64_max_plus_1	{"v": 18446744073709552000}	18446744073709551616
11	int64_min	{"v": -9223372036854775808}	-9223372036854775808
-- !result
select k, label, variant_typeof(variant_query(cast(j as variant), '$.v')) as v_type
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	Int64
2	int64_max	Int64
3	small	Int64
4	pow63	Decimal16
5	int64_max_plus_2	Decimal16
6	pow63_plus_2048	Decimal16
7	uint64_max_minus_5	Decimal16
8	uint64_max_minus_1	Decimal16
9	uint64_max	Decimal16
10	uint64_max_plus_1	Double
11	int64_min	Int64
-- !result
select k, label, cast(cast(j as variant) as varchar) as v_text from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	{"v":9223372036854775806}
2	int64_max	{"v":9223372036854775807}
3	small	{"v":42}
4	pow63	{"v":9223372036854775808}
5	int64_max_plus_2	{"v":9223372036854775809}
6	pow63_plus_2048	{"v":9223372036854777856}
7	uint64_max_minus_5	{"v":18446744073709551610}
8	uint64_max_minus_1	{"v":18446744073709551614}
9	uint64_max	{"v":18446744073709551615}
10	uint64_max_plus_1	{"v":1.8446744073709552e+19}
11	int64_min	{"v":-9223372036854775808}
-- !result
select k, label, variant_typeof(cast(l as variant)) as l_type, cast(cast(l as variant) as varchar) as l_text
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	Decimal16	9223372036854775806
2	int64_max	Decimal16	9223372036854775807
3	small	Decimal16	42
4	pow63	Decimal16	9223372036854775808
5	int64_max_plus_2	Decimal16	9223372036854775809
6	pow63_plus_2048	Decimal16	9223372036854777856
7	uint64_max_minus_5	Decimal16	18446744073709551610
8	uint64_max_minus_1	Decimal16	18446744073709551614
9	uint64_max	Decimal16	18446744073709551615
10	uint64_max_plus_1	Decimal16	18446744073709551616
11	int64_min	Decimal16	-9223372036854775808
-- !result
select k, label,
       cast(cast(cast(l as json) as variant) as varchar) as via_json,
       cast(cast(l as variant) as varchar) as direct
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	9223372036854775806	9223372036854775806
2	int64_max	9223372036854775807	9223372036854775807
3	small	42	42
4	pow63	9223372036854775808	9223372036854775808
5	int64_max_plus_2	9223372036854775809	9223372036854775809
6	pow63_plus_2048	9223372036854777856	9223372036854777856
7	uint64_max_minus_5	18446744073709551610	18446744073709551610
8	uint64_max_minus_1	18446744073709551614	18446744073709551614
9	uint64_max	18446744073709551615	18446744073709551615
10	uint64_max_plus_1	None	18446744073709551616
11	int64_min	-9223372036854775808	-9223372036854775808
-- !result
select count(*) as rows_total,
       count(distinct cast(j as varchar)) as distinct_json,
       count(distinct cast(cast(j as variant) as varchar)) as distinct_variant
  from t1_${uuid0};
-- result:
11	11	11
-- !result
select cast(cast(j as variant) as varchar) as v_text, count(*) as rows_sharing_it
  from t1_${uuid0} group by v_text order by v_text;
-- result:
{"v":-9223372036854775808}	1
{"v":1.8446744073709552e+19}	1
{"v":18446744073709551610}	1
{"v":18446744073709551614}	1
{"v":18446744073709551615}	1
{"v":42}	1
{"v":9223372036854775806}	1
{"v":9223372036854775807}	1
{"v":9223372036854775808}	1
{"v":9223372036854775809}	1
{"v":9223372036854777856}	1
-- !result
select case when cast(a.j as variant) = cast(b.j as variant) then 'SAME' else 'DIFFERENT' end as variant_cmp,
       case when a.j = b.j then 'SAME' else 'DIFFERENT' end as json_cmp
  from t1_${uuid0} a, t1_${uuid0} b where a.k = 7 and b.k = 9;
-- result:
DIFFERENT	DIFFERENT
-- !result
select k, label from t1_${uuid0}
  where cast(j as variant) = cast(parse_json('{"v":18446744073709551610}') as variant) order by k;
-- result:
7	uint64_max_minus_5
-- !result
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as largeint) as as_largeint,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,0)) as as_decimal_38_0,
       cast(variant_query(cast(j as variant), '$.v') as decimal(20,0)) as as_decimal_20_0
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	9223372036854775806	9223372036854775806	9223372036854775806
2	int64_max	9223372036854775807	9223372036854775807	9223372036854775807
3	small	42	42	42
4	pow63	9223372036854775808	9223372036854775808	9223372036854775808
5	int64_max_plus_2	9223372036854775809	9223372036854775809	9223372036854775809
6	pow63_plus_2048	9223372036854777856	9223372036854777856	9223372036854777856
7	uint64_max_minus_5	18446744073709551610	18446744073709551610	18446744073709551610
8	uint64_max_minus_1	18446744073709551614	18446744073709551614	18446744073709551614
9	uint64_max	18446744073709551615	18446744073709551615	18446744073709551615
10	uint64_max_plus_1	18446744073709551616	18446744073709551616	18446744073709551616
11	int64_min	-9223372036854775808	-9223372036854775808	-9223372036854775808
-- !result
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as decimal(18,0)) as as_decimal_18_0,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,20)) as as_decimal_38_20,
       cast(variant_query(cast(j as variant), '$.v') as decimal(38,2)) as as_decimal_38_2
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	9223372036854775806	None	9223372036854775806.00
2	int64_max	9223372036854775807	None	9223372036854775807.00
3	small	42	42.00000000000000000000	42.00
4	pow63	None	None	9223372036854775808.00
5	int64_max_plus_2	None	None	9223372036854775809.00
6	pow63_plus_2048	None	None	9223372036854777856.00
7	uint64_max_minus_5	None	None	18446744073709551610.00
8	uint64_max_minus_1	None	None	18446744073709551614.00
9	uint64_max	None	None	18446744073709551615.00
10	uint64_max_plus_1	None	None	18446744073709551616.00
11	int64_min	-9223372036854775808	None	-9223372036854775808.00
-- !result
select k, label,
       cast(cast(variant_query(cast(j as variant), '$.v') as double) as varchar) as as_double,
       cast(get_variant_double(cast(j as variant), '$.v') as varchar) as get_double,
       get_variant_string(cast(j as variant), '$.v') as get_string
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	9.223372036854776e+18	9.223372036854776e+18	9223372036854775806
2	int64_max	9.223372036854776e+18	9.223372036854776e+18	9223372036854775807
3	small	42	42	42
4	pow63	9.223372036854776e+18	9.223372036854776e+18	9223372036854775808
5	int64_max_plus_2	9.223372036854776e+18	9.223372036854776e+18	9223372036854775809
6	pow63_plus_2048	9.223372036854778e+18	9.223372036854778e+18	9223372036854777856
7	uint64_max_minus_5	1.8446744073709552e+19	1.8446744073709552e+19	18446744073709551610
8	uint64_max_minus_1	1.8446744073709552e+19	1.8446744073709552e+19	18446744073709551614
9	uint64_max	1.8446744073709552e+19	1.8446744073709552e+19	18446744073709551615
10	uint64_max_plus_1	1.8446744073709552e+19	1.8446744073709552e+19	1.8446744073709552e+19
11	int64_min	-9.223372036854776e+18	-9.223372036854776e+18	-9223372036854775808
-- !result
select k, label,
       cast(variant_query(cast(j as variant), '$.v') as bigint) as as_bigint,
       get_variant_int(cast(j as variant), '$.v') as get_int
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	9223372036854775806	9223372036854775806
2	int64_max	9223372036854775807	9223372036854775807
3	small	42	42
4	pow63	-9223372036854775808	-9223372036854775808
5	int64_max_plus_2	-9223372036854775807	-9223372036854775807
6	pow63_plus_2048	-9223372036854773760	-9223372036854773760
7	uint64_max_minus_5	-6	-6
8	uint64_max_minus_1	-2	-2
9	uint64_max	-1	-1
10	uint64_max_plus_1	-9223372036854775808	-9223372036854775808
11	int64_min	-9223372036854775808	-9223372036854775808
-- !result
create table t2_${uuid0} (k int, j json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
-- result:
-- !result
insert into t2_${uuid0} values
  (1, parse_json('[18446744073709551615,1]')),
  (2, parse_json('{"a":{"b":18446744073709551615}}')),
  (3, parse_json('{"a":[9223372036854775809,18446744073709551615]}')),
  (4, parse_json('[9223372036854775807,42]'));
-- result:
-- !result
select k, cast(cast(j as variant) as varchar) as v_text from t2_${uuid0} order by k;
-- result:
1	[18446744073709551615,1]
2	{"a":{"b":18446744073709551615}}
3	{"a":[9223372036854775809,18446744073709551615]}
4	[9223372036854775807,42]
-- !result
select k, cast(cast(map{'m': j} as variant) as varchar) as map_text from t2_${uuid0} order by k;
-- result:
1	{"m":[18446744073709551615,1]}
2	{"m":{"a":{"b":18446744073709551615}}}
3	{"m":{"a":[9223372036854775809,18446744073709551615]}}
4	{"m":[9223372036854775807,42]}
-- !result
select k, label, variant_typeof(cast(json_query(j, '$.v') as variant)) as scalar_type,
       cast(cast(json_query(j, '$.v') as variant) as varchar) as scalar_text
  from t1_${uuid0} order by k;
-- result:
1	int64_max_minus_1	Int64	9223372036854775806
2	int64_max	Int64	9223372036854775807
3	small	Int64	42
4	pow63	Decimal16	9223372036854775808
5	int64_max_plus_2	Decimal16	9223372036854775809
6	pow63_plus_2048	Decimal16	9223372036854777856
7	uint64_max_minus_5	Decimal16	18446744073709551610
8	uint64_max_minus_1	Decimal16	18446744073709551614
9	uint64_max	Decimal16	18446744073709551615
10	uint64_max_plus_1	Double	1.8446744073709552e+19
11	int64_min	Int64	-9223372036854775808
-- !result
create table t3_${uuid0} (k int, j json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="true");
-- result:
-- !result
insert into t3_${uuid0} values
  (1, parse_json('{"v":9223372036854775807}')),
  (2, parse_json('{"v":9223372036854775809}')),
  (3, parse_json('{"v":18446744073709551615}')),
  (4, parse_json('{"v":18446744073709551616}'));
-- result:
-- !result
select k, cast(j->'$.v' as largeint) as flat_largeint,
       cast(cast(j->'$.v' as double) as varchar) as flat_double,
       cast(j as varchar) as j_text
  from t3_${uuid0} order by k;
-- result:
1	9223372036854775807	9.223372036854776e+18	{"v": 9223372036854775807}
2	9223372036854775809	9.223372036854776e+18	{"v": 9223372036854775809}
3	18446744073709551615	1.8446744073709552e+19	{"v": 18446744073709551615}
4	18446744073709551616	1.8446744073709552e+19	{"v": 18446744073709552000}
-- !result
select k, cast(cast(j as variant) as varchar) as v_text from t3_${uuid0} order by k;
-- result:
1	{"v":9223372036854775807}
2	{"v":9223372036854775809}
3	{"v":18446744073709551615}
4	{"v":1.8446744073709552e+19}
-- !result
