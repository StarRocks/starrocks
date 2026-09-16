-- name: test_agg_state_low_cardinality_dict
-- An aggregate-state column stores a serialized aggregate state, not the values its type
-- describes. The BE reads it through the agg state descriptor and never looks at the column
-- type, so it must never be dictionary encoded.
--
-- Getting the FE to offer a dictionary for such a column takes a specific sequence: several
-- loads, a compaction so the column is written as one dictionary encoded segment, then more
-- loads so the storage layer still has to merge at read time. The predicate below is what
-- keeps the column dictionary encoded; reading it through array_agg_distinct_merge would put
-- it on the decode side instead and would prove nothing.
CREATE TABLE `t_src` (
  `id` bigint,
  `k1` bigint,
  `s` varchar(100)
) DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
insert into t_src select gs, gs % 2000, concat('v', gs % 3) from (select generate_series as gs from TABLE(generate_series(0, 49999))) g;
-- result:
-- !result
CREATE TABLE `t_state` (
  `k1` bigint,
  `v_str` array_agg_distinct(varchar(100))
) DISTRIBUTED BY HASH(`k1`) BUCKETS 4
PROPERTIES ("replication_num" = "1");
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
[UC] alter table t_state compact;
[UC] analyze full table t_src;
[UC] analyze full table t_state;
function: wait_global_dict_ready('s', 't_src')
-- result:

-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
insert into t_state select k1, array_agg_distinct_state(s) from t_src;
-- result:
-- !result
select count(*) from t_state;
-- result:
2000
-- !result
select count(*) from t_state where v_str[1] = 'v0' or v_str[2] = 'v0' or v_str[3] = 'v0';
-- result:
2000
-- !result
select count(*) from t_state where v_str[1] = 'nope';
-- result:
0
-- !result
select k1, array_sort(v_str) from t_state order by k1 limit 3;
-- result:
0	["v0","v1","v2"]
1	["v0","v1","v2"]
2	["v0","v1","v2"]
-- !result
