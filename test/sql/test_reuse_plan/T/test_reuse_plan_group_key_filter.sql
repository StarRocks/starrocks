-- name: test_reuse_plan_group_key_filter
-- Test Point:
--   1. Common plan extraction fuses pieces that differ only by conjuncts shared by all of them or by
--      conjuncts on group-by keys, keeping plain aggregates and applying the key conjuncts on the consumer.
--   2. The fused result equals the unfused one, including NULL group keys, conjuncts evaluating to NULL,
--      groups without any qualifying row, and pieces that mix key-only and row conjuncts.
-- Method: run every query with cbo_extract_common_plan on and off; both runs must return the same rows.
--         EXPLAIN checks that the key-only shapes are fused without a row_hit gate.
-- Scope: ReuseFusionPlanRule

create database db_${uuid0};
use db_${uuid0};

create table t0 (k1 int, k2 int, v int) duplicate key(k1) distributed by hash(k1) buckets 3 properties ("replication_num" = "1");
create table t1 (k int, w int) duplicate key(k) distributed by hash(k) buckets 3 properties ("replication_num" = "1");
insert into t0 values (1, 1, 5), (1, 1, 20), (1, 2, null), (2, 1, 30), (2, 2, 1), (3, 1, 12), (3, null, 40), (4, 2, 2), (4, 2, 3), (null, 1, 7), (null, 1, 15), (null, null, null), (5, null, 11), (6, 1, null);
insert into t1 values (1, 100), (2, 200), (null, 300);

set cbo_extract_common_plan = true;

-- key-only conjunct on one piece
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3;
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
function: assert_explain_not_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3', 'row_hit', '_if(')

-- IS NULL on a key keeps exactly the NULL group
select * from (select 'a' p, k1, count(*) c from t0 group by k1 union all select 'b', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3;
function: assert_explain_contains('select * from (select \'a\' p, k1, count(*) c from t0 group by k1 union all select \'b\', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
function: assert_explain_not_contains('select * from (select \'a\' p, k1, count(*) c from t0 group by k1 union all select \'b\', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3', 'row_hit', '_if(')

-- a key expression that can be UNKNOWN, ORed with IS NULL
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3;
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')
function: assert_explain_not_contains('select * from (select \'a\' p, k1, sum(v) s from t0 group by k1 union all select \'b\', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3', 'row_hit', '_if(')

-- key-only and row conjuncts on one piece; some groups have no qualifying row
select * from (select 'a' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select 'b', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3;
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select \'b\', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')

-- three pieces over a shared conjunct: none / key-only / row residual
select * from (select 'a' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select 'b', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select 'c', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3;
function: assert_explain_contains('select * from (select \'a\' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select \'b\', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select \'c\', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3', 'MultiCastDataSinks')

-- two group-by keys, the key-only conjunct is on the second key and the key order differs
select * from (select 'a' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select 'b', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4;
function: assert_explain_contains('select * from (select \'a\' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select \'b\', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4', 'MultiCastDataSinks')
function: assert_explain_not_contains('select * from (select \'a\' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select \'b\', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4', 'row_hit', '_if(')

-- the consumer does not select the key its gate uses
select * from (select 'a' p, s from (select k1, sum(v) s from t0 group by k1) x union all select 'b', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2;
function: assert_explain_contains('select * from (select \'a\' p, s from (select k1, sum(v) s from t0 group by k1) x union all select \'b\', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2', 'MultiCastDataSinks')
function: assert_explain_not_contains('select * from (select \'a\' p, s from (select k1, sum(v) s from t0 group by k1) x union all select \'b\', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2', 'row_hit', '_if(')

-- TPC-DS Q65 shape: the same subquery twice, one side joined on a nullable key
select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3;
function: assert_explain_contains('select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3', 'MultiCastDataSinks')

set cbo_extract_common_plan = false;

-- key-only conjunct on one piece
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 > 2 group by k1) t order by 1, 2, 3;

-- IS NULL on a key keeps exactly the NULL group
select * from (select 'a' p, k1, count(*) c from t0 group by k1 union all select 'b', k1, count(*) from t0 where k1 is null group by k1) t order by 1, 2, 3;

-- a key expression that can be UNKNOWN, ORed with IS NULL
select * from (select 'a' p, k1, sum(v) s from t0 group by k1 union all select 'b', k1, sum(v) from t0 where k1 + 1 > 3 or k1 is null group by k1) t order by 1, 2, 3;

-- key-only and row conjuncts on one piece; some groups have no qualifying row
select * from (select 'a' p, k1, sum(v) s, count(*) c from t0 group by k1 union all select 'b', k1, sum(v), count(*) from t0 where k1 > 1 and v > 10 group by k1) t order by 1, 2, 3;

-- three pieces over a shared conjunct: none / key-only / row residual
select * from (select 'a' p, k1, sum(v) s from t0 where k2 = 1 group by k1 union all select 'b', k1, sum(v) from t0 where k2 = 1 and k1 >= 2 group by k1 union all select 'c', k1, sum(v) from t0 where k2 = 1 and v > 10 group by k1) t order by 1, 2, 3;

-- two group-by keys, the key-only conjunct is on the second key and the key order differs
select * from (select 'a' p, k1, k2, sum(v) s from t0 group by k1, k2 union all select 'b', k1, k2, sum(v) from t0 where k2 is not null group by k2, k1) t order by 1, 2, 3, 4;

-- the consumer does not select the key its gate uses
select * from (select 'a' p, s from (select k1, sum(v) s from t0 group by k1) x union all select 'b', s from (select k1, sum(v) s from t0 where k1 > 2 group by k1) y) t order by 1, 2;

-- TPC-DS Q65 shape: the same subquery twice, one side joined on a nullable key
select a.k1, a.k2, a.s from (select k1, k2, sum(v) s from t0 group by k1, k2) a, (select k1, max(s) mx from (select k1, k2, sum(v) s from t0 group by k1, k2) x group by k1) b, t1 where a.k1 = b.k1 and a.k2 = t1.k and a.s * 2 > b.mx order by 1, 2, 3;

drop database db_${uuid0} force;
