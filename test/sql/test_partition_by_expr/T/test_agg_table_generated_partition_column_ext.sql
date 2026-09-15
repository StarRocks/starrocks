-- name: test_agg_gpc_real_replace_column_still_guarded
-- The hidden partition column carries REPLACE, but a table may also carry a user REPLACE column.
-- Dropping a key column must stay forbidden in that case: only the hidden column may be ignored.
create table agg_repl (
  event_day datetime not null,
  city varchar(64) not null,
  channel varchar(64) not null,
  pv bigint sum,
  tag varchar(64) replace
) aggregate key(event_day, city, channel)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_repl values ('2026-07-13 01:00:00', 'sh', 'app', 1, 'a'), ('2026-07-20 01:00:00', 'sh', 'web', 2, 'b');
insert into agg_repl values ('2026-07-13 01:00:00', 'sh', 'app', 1, 'c');
select event_day, city, channel, pv, tag from agg_repl order by event_day, city, channel;
alter table agg_repl drop column channel;

-- name: test_agg_gpc_compaction
create table agg_compact (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  uv bigint max
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_compact values ('2026-07-13 01:00:00', 'sh', 1, 10), ('2026-07-20 01:00:00', 'bj', 1, 10);
insert into agg_compact values ('2026-07-13 01:00:00', 'sh', 1, 20), ('2026-07-20 01:00:00', 'bj', 1, 20);
insert into agg_compact values ('2026-07-13 01:00:00', 'sh', 1, 30), ('2026-07-20 01:00:00', 'bj', 1, 30);
insert into agg_compact values ('2026-07-13 01:00:00', 'sh', 1, 40), ('2026-07-20 01:00:00', 'bj', 1, 40);
insert into agg_compact values ('2026-07-13 01:00:00', 'sh', 1, 50), ('2026-07-20 01:00:00', 'bj', 1, 50);
select event_day, city, pv, uv from agg_compact order by event_day, city;
-- merging five rowsets must not change the answer
alter table agg_compact compact;
select event_day, city, pv, uv from agg_compact order by event_day, city;
select count(*), sum(pv), max(uv) from agg_compact;

-- name: test_agg_gpc_delete
create table agg_del (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_del values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-13 02:00:00', 'bj', 2),
('2026-07-20 01:00:00', 'sh', 4),
('2026-07-27 01:00:00', 'bj', 8);
delete from agg_del where city = 'bj';
select event_day, city, pv from agg_del order by event_day, city;
delete from agg_del where event_day = '2026-07-20 01:00:00';
select event_day, city, pv from agg_del order by event_day, city;

-- name: test_agg_gpc_insert_overwrite
create table agg_ow (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_ow values ('2026-07-13 01:00:00', 'sh', 1), ('2026-07-20 01:00:00', 'sh', 2);
select event_day, city, pv from agg_ow order by event_day;
-- overwriting one partition must not disturb the other, and must route through the same expression
insert overwrite agg_ow partition(p20260713000000) values ('2026-07-14 01:00:00', 'sh', 100);
select event_day, city, pv from agg_ow order by event_day;
-- a whole table overwrite may also create a brand new partition
insert overwrite agg_ow values ('2026-08-03 01:00:00', 'bj', 7);
select event_day, city, pv from agg_ow order by event_day;
select partition_name from information_schema.partitions_meta
where table_name = 'agg_ow' order by partition_name;

-- name: test_agg_gpc_column_list_and_ctas_like
create table agg_src (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  uv bigint max
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- the user never supplies the generated column, not even with an explicit column list
insert into agg_src (event_day, city, pv) values ('2026-07-13 01:00:00', 'sh', 1);
insert into agg_src (city, event_day, uv) values ('sh', '2026-07-13 01:00:00', 9);
select event_day, city, pv, uv from agg_src order by event_day;
-- CREATE TABLE LIKE must copy the partition expression, not the hidden column
create table agg_like like agg_src;
insert into agg_like select event_day, city, pv, uv from agg_src;
select event_day, city, pv, uv from agg_like order by event_day;
select partition_name from information_schema.partitions_meta
where table_name = 'agg_like' order by partition_name;

-- name: test_agg_gpc_other_agg_types
create table agg_types (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  last_tag varchar(64) replace_if_not_null,
  uv_bitmap bitmap bitmap_union,
  uv_hll hll hll_union
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_types values
('2026-07-13 01:00:00', 'sh', 1, 'x', to_bitmap(1), hll_hash(1)),
('2026-07-13 01:00:00', 'sh', 2, null, to_bitmap(2), hll_hash(2)),
('2026-07-20 01:00:00', 'bj', 4, 'y', to_bitmap(3), hll_hash(3));
select event_day, city, pv, last_tag, bitmap_count(uv_bitmap), hll_cardinality(uv_hll)
from agg_types order by event_day, city;

-- name: test_agg_gpc_other_partition_exprs
create table agg_substr (
  city varchar(64) not null,
  event_day datetime not null,
  pv bigint sum
) aggregate key(city, event_day)
partition by substr(city, 1, 2)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_substr values ('shanghai', '2026-07-13 01:00:00', 1), ('shenzhen', '2026-07-13 01:00:00', 2), ('beijing', '2026-07-13 01:00:00', 4);
select city, pv from agg_substr order by city;
select partition_name from information_schema.partitions_meta
where table_name = 'agg_substr' order by partition_name;
create table agg_unixtime (
  ts bigint not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(ts, city)
partition by from_unixtime(ts)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_unixtime values (unix_timestamp('2026-07-13 01:00:00'), 'sh', 1), (unix_timestamp('2026-07-20 01:00:00'), 'sh', 2);
select ts, city, pv from agg_unixtime order by ts;
create table agg_quarter (
  event_day date not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('quarter', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_quarter values ('2026-02-01', 'sh', 1), ('2026-05-01', 'sh', 2), ('2026-02-15', 'sh', 4);
select event_day, city, pv from agg_quarter order by event_day;
select partition_name from information_schema.partitions_meta
where table_name = 'agg_quarter' order by partition_name;

-- name: test_agg_gpc_partition_lifecycle
create table agg_life (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_life values ('2026-07-13 01:00:00', 'sh', 1), ('2026-07-20 01:00:00', 'sh', 2);
-- adding a partition by hand must use the generated column value
alter table agg_life add partition p20260601000000 values in ("2026-06-01 00:00:00");
select partition_name from information_schema.partitions_meta
where table_name = 'agg_life' order by partition_name;
insert into agg_life values ('2026-06-03 01:00:00', 'sh', 16);
select event_day, city, pv from agg_life order by event_day;
alter table agg_life drop partition p20260601000000;
select event_day, city, pv from agg_life order by event_day;
truncate table agg_life;
select count(*) from agg_life;

-- name: test_agg_gpc_preaggregation_and_query_cache
create table agg_cache (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_cache values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-13 02:00:00', 'sh', 2),
('2026-07-20 01:00:00', 'bj', 4);
-- the hidden REPLACE column must not turn pre-aggregation off
function: assert_explain_contains("select city, sum(pv) from agg_cache group by city", "PREAGGREGATION: ON")
set enable_query_cache = true;
select city, sum(pv) from agg_cache group by city order by city;
select city, sum(pv) from agg_cache group by city order by city;
select sum(pv) from agg_cache where event_day >= '2026-07-20';
set enable_query_cache = false;
select city, sum(pv) from agg_cache group by city order by city;

-- name: test_agg_gpc_stream_load
create database test_agg_gpc_stream_load_db;
use test_agg_gpc_stream_load_db;
create table agg_sl (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  uv bigint max
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- the loader must fill the generated column itself, the file only carries the declared columns
shell: curl --location-trusted -u root: -T ${root_path}/lib/../common/data/stream_load/sr_agg_expr_partition.csv -XPUT -H expect:100-continue -H label:sr_agg_expr_partition_1 -H column_separator:, ${url}/api/test_agg_gpc_stream_load_db/agg_sl/_stream_load
sync;
select event_day, city, pv, uv from agg_sl order by event_day, city;
select partition_name from information_schema.partitions_meta
where table_name = 'agg_sl' order by partition_name;
drop database test_agg_gpc_stream_load_db;
