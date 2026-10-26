-- name: test_variant_conditional_exprs
-- VARIANT was missing from two BE type-dispatch tables, so the whole FE->BE chain for these
-- expressions was unreachable even though the row-wise evaluation path behind them was already
-- correct. `SWITCH_ALL_WHEN_TYPE` in case_expr_tpl.hpp had no TYPE_VARIANT, so a simple
-- `CASE v WHEN ... END` over a VARIANT compared its WHEN branches as a VARIANT and the factory
-- returned nullptr; `CASE_ALL_TYPE` in condition_expr.cpp had no TYPE_VARIANT either, and
-- if/ifnull/nullif/coalesce had no VARIANT signature registered in functions.py at all.
-- A searched `CASE WHEN c THEN v END` already worked, because only its RESULT type is VARIANT.
create database db_${uuid0};
use db_${uuid0};
create table t1 (k int, ja json, jb json, jc json) duplicate key(k)
  distributed by hash(k) buckets 1 properties("replication_num"="1","flat_json.enable"="false");
-- k=1 has a=b, k=2 has all three distinct, k=3 has a NULL, k=4 has a and b NULL.
insert into t1 values
  (1, parse_json('{"v":1}'), parse_json('{"v":1}'), parse_json('{"v":300}')),
  (2, parse_json('{"v":2}'), parse_json('{"v":20}'), parse_json('{"v":301}')),
  (3, null,                  parse_json('{"v":21}'), parse_json('{"v":302}')),
  (4, null,                  null,                   parse_json('{"v":303}'));

-- The searched form was already reachable before this change and must not move.
select k, cast(case when k % 2 = 1 then cast(ja as variant) else cast(jb as variant) end as varchar) as r
  from t1 order by k;

-- Simple CASE whose comparison type is VARIANT. This is the form that returned
-- "vectorized engine case expr no support when type" before TYPE_VARIANT was added to
-- SWITCH_ALL_WHEN_TYPE. k=1 matches the first WHEN, the rest fall through to ELSE.
select k, cast(case cast(ja as variant) when cast(jb as variant) then cast(jc as variant)
                                        else cast(ja as variant) end as varchar) as r
  from t1 order by k;

-- Same dispatch, but with a flat result type: the WHEN side alone forces the row-wise branch, so
-- the INT result column has to be built through Column::append() rather than a ColumnBuilder.
select k, case cast(ja as variant) when cast(jb as variant) then 1 else 0 end as r
  from t1 order by k;

-- if / ifnull / nullif / coalesce over VARIANT. Each needed both a functions.py signature and
-- TYPE_VARIANT in CASE_ALL_TYPE; without either one the FE found no overload, or the BE fell back
-- to VectorizedFunctionCallExpr and never reached the row-wise branch.
select k, cast(if(k % 2 = 1, cast(ja as variant), cast(jb as variant)) as varchar) as r
  from t1 order by k;
select k, cast(ifnull(cast(ja as variant), cast(jc as variant)) as varchar) as r
  from t1 order by k;
-- nullif returns NULL where the two sides are equal, so only k=1 is NULL here.
select k, cast(nullif(cast(ja as variant), cast(jb as variant)) as varchar) as r
  from t1 order by k;
select k, cast(coalesce(cast(ja as variant), cast(jb as variant), cast(jc as variant)) as varchar) as r
  from t1 order by k;

-- The results have to stay VARIANT rather than degrade to JSON or VARCHAR on the way through.
select k, variant_typeof(if(k % 2 = 1, cast(ja as variant), cast(jb as variant))) as if_type,
          variant_typeof(coalesce(cast(ja as variant), cast(jc as variant))) as coalesce_type
  from t1 order by k;

-- Reading a path back out of the result proves the shredded sub-columns survived the row-wise
-- append; a result that kept only ObjectColumn::_pool would lose them.
select k, cast(variant_query(coalesce(cast(ja as variant), cast(jc as variant)), '$.v') as varchar) as v
  from t1 order by k;

drop database db_${uuid0};
