-- name: test_variant_conditional_exprs
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
create table t1 (k int, ja json, jb json, jc json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
-- result:
-- !result
insert into t1 values
  (1, parse_json('{"v":1}'), parse_json('{"v":1}'), parse_json('{"v":300}')),
  (2, parse_json('{"v":2}'), parse_json('{"v":20}'), parse_json('{"v":301}')),
  (3, null,                  parse_json('{"v":21}'), parse_json('{"v":302}')),
  (4, null,                  null,                   parse_json('{"v":303}'));
-- result:
-- !result
select k, cast(case when k % 2 = 1 then cast(ja as variant) else cast(jb as variant) end as varchar) as r
  from t1 order by k;
-- result:
1	{"v":1}
2	{"v":20}
3	None
4	None
-- !result
select k, cast(case cast(ja as variant) when cast(jb as variant) then cast(jc as variant)
                                        else cast(ja as variant) end as varchar) as r
  from t1 order by k;
-- result:
1	{"v":300}
2	{"v":2}
3	None
4	None
-- !result
select k, case cast(ja as variant) when cast(jb as variant) then 1 else 0 end as r
  from t1 order by k;
-- result:
1	1
2	0
3	0
4	0
-- !result
select k, cast(if(k % 2 = 1, cast(ja as variant), cast(jb as variant)) as varchar) as r
  from t1 order by k;
-- result:
1	{"v":1}
2	{"v":20}
3	None
4	None
-- !result
select k, cast(ifnull(cast(ja as variant), cast(jc as variant)) as varchar) as r
  from t1 order by k;
-- result:
1	{"v":1}
2	{"v":2}
3	{"v":302}
4	{"v":303}
-- !result
select k, cast(nullif(cast(ja as variant), cast(jb as variant)) as varchar) as r
  from t1 order by k;
-- result:
1	None
2	{"v":2}
3	None
4	None
-- !result
select k, cast(coalesce(cast(ja as variant), cast(jb as variant), cast(jc as variant)) as varchar) as r
  from t1 order by k;
-- result:
1	{"v":1}
2	{"v":2}
3	{"v":21}
4	{"v":303}
-- !result
select k, variant_typeof(if(k % 2 = 1, cast(ja as variant), cast(jb as variant))) as if_type,
          variant_typeof(coalesce(cast(ja as variant), cast(jc as variant))) as coalesce_type
  from t1 order by k;
-- result:
1	Object	Object
2	Object	Object
3	None	Object
4	None	Object
-- !result
select k, cast(variant_query(coalesce(cast(ja as variant), cast(jc as variant)), '$.v') as varchar) as v
  from t1 order by k;
-- result:
1	1
2	2
3	302
4	303
-- !result
drop database db_${uuid0};
-- result:
-- !result