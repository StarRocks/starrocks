-- name: test_reuse_plan_varbinary_agg
-- Test Point:
--   1. Common plan extraction leaves two pieces unfused when the aggregate's argument type has no
--      filtered form, and the query still executes instead of failing with an unbound function call.
--   2. The same query shape over an aggregate that does have a filtered form is still fused.
-- Method: run both shapes with cbo_extract_common_plan on, and assert MultiCastDataSinks is absent
--         from the first EXPLAIN and present in the second.
-- Scope: ReuseFusionPlanRule

create database db_${uuid0};
use db_${uuid0};

create table t0 (k1 int, k2 int) distributed by hash(k1) buckets 3
properties ("replication_num" = "1");

insert into t0 values (1, 1), (2, 1), (3, 2), (4, 2), (5, 2);

set cbo_extract_common_plan = true;

-- ds_hll_accumulate's VARBINARY state has neither an `_if` variant nor an if(BOOLEAN, VARBINARY, VARBINARY) builtin
with a as (select ds_hll_accumulate(k1) as sk from t0 where k2 = 1),
     b as (select ds_hll_accumulate(k1) as sk from t0 where k2 = 2)
select cast(ds_hll_estimate(a.sk) as int), cast(ds_hll_estimate(b.sk) as int) from a, b;
function: assert_explain_not_contains('with a as (select ds_hll_accumulate(k1) as sk from t0 where k2 = 1), b as (select ds_hll_accumulate(k1) as sk from t0 where k2 = 2) select cast(ds_hll_estimate(a.sk) as int), cast(ds_hll_estimate(b.sk) as int) from a, b', 'MultiCastDataSinks')

select a.s, b.s from (select sum(k1) as s from t0 where k2 = 1) a, (select sum(k1) as s from t0 where k2 = 2) b;
function: assert_explain_contains('select a.s, b.s from (select sum(k1) as s from t0 where k2 = 1) a, (select sum(k1) as s from t0 where k2 = 2) b', 'MultiCastDataSinks')

drop database db_${uuid0} force;
