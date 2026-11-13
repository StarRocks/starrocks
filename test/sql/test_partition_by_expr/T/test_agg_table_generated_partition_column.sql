-- name: test_agg_table_generated_partition_column
create table agg_week (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  uv bigint max
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- the generated partition column must stay invisible to users
desc agg_week;
insert into agg_week values
('2026-07-13 01:00:00', 'sh', 1, 10),
('2026-07-15 02:00:00', 'sh', 2, 20),
('2026-07-19 23:59:59', 'bj', 4, 40),
('2026-07-20 00:00:00', 'sh', 8, 80),
('2026-07-26 12:00:00', 'bj', 16, 160),
('2026-08-03 00:00:00', 'sh', 32, 320);
-- one partition per ISO week, named after the Monday of that week
select partition_name from information_schema.partitions_meta
where table_name = 'agg_week' order by partition_name;
select * from agg_week order by event_day, city;
-- load the same keys again, rows must aggregate instead of duplicating and must stay in their partition
insert into agg_week values
('2026-07-13 01:00:00', 'sh', 1, 5),
('2026-07-15 02:00:00', 'sh', 3, 30),
('2026-08-03 00:00:00', 'sh', 32, 160);
select event_day, city, pv, uv from agg_week order by event_day, city;
select count(*), sum(pv), max(uv) from agg_week;

-- name: test_agg_table_generated_partition_column_ddl @native
create table agg_week_ddl (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- SHOW CREATE TABLE must round trip the partition expression and never expose the generated column
show create table agg_week_ddl;

-- name: test_agg_table_generated_partition_column_prune
create table agg_week_prune (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_week_prune values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-20 01:00:00', 'sh', 2),
('2026-07-27 01:00:00', 'sh', 4),
('2026-08-03 01:00:00', 'sh', 8);
select event_day, pv from agg_week_prune where event_day >= '2026-07-20' and event_day < '2026-07-27' order by event_day;
select sum(pv) from agg_week_prune where event_day >= '2026-07-27';
select sum(pv) from agg_week_prune where event_day = '2026-07-20 01:00:00';
select city, sum(pv) from agg_week_prune where event_day >= '2026-07-20' group by city order by city;

-- name: test_agg_table_generated_partition_column_alter
create table agg_week_alter (
  event_day datetime not null,
  city varchar(64) not null,
  channel varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city, channel)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_week_alter values ('2026-07-13 01:00:00', 'sh', 'app', 1), ('2026-07-20 01:00:00', 'sh', 'web', 2);
-- a new value column must be placed before the hidden partition column
alter table agg_week_alter add column clicks bigint sum default "0";
function: wait_alter_table_finish()
desc agg_week_alter;
insert into agg_week_alter values ('2026-07-13 01:00:00', 'sh', 'app', 1, 7);
select event_day, city, channel, pv, clicks from agg_week_alter order by event_day, city, channel;
-- a rollup on a table carrying the hidden partition column
alter table agg_week_alter add rollup r_city (city, pv);
function: wait_alter_table_finish("ROLLUP", 8)
select city, sum(pv) from agg_week_alter group by city order by city;
-- the hidden REPLACE column must not block dropping a key column
alter table agg_week_alter drop column channel;
function: wait_alter_table_finish()
select event_day, city, pv, clicks from agg_week_alter order by event_day, city;

-- name: test_agg_table_multi_expression_partition
create table agg_multi_expr (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by (city, date_trunc('day', event_day))
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into agg_multi_expr values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-13 02:00:00', 'sh', 2),
('2026-07-13 03:00:00', 'bj', 4),
('2026-07-14 03:00:00', 'bj', 8);
select partition_name from information_schema.partitions_meta
where table_name = 'agg_multi_expr' order by partition_name;
select event_day, city, pv from agg_multi_expr order by event_day, city;

-- name: test_unique_table_generated_partition_column
create table uniq_week (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint
) unique key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
insert into uniq_week values ('2026-07-13 01:00:00', 'sh', 1), ('2026-07-20 01:00:00', 'sh', 2);
insert into uniq_week values ('2026-07-13 01:00:00', 'sh', 100);
select event_day, city, pv from uniq_week order by event_day, city;

-- name: test_agg_table_generated_partition_column_invalid
-- the partition expression may only reference key columns
create table agg_bad_expr (
  event_day datetime not null,
  city varchar(64) not null,
  last_day datetime max,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', last_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- user defined generated columns stay unsupported on aggregate tables
create table agg_user_gencol (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  week_start datetime null as date_trunc('week', event_day)
) aggregate key(event_day, city)
partition by date_trunc('day', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
