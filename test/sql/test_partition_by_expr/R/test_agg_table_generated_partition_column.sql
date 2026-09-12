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
-- result:
-- !result
desc agg_week;
-- result:
event_day	datetime	NO	true	None	
city	varchar(64)	NO	true	None	
pv	bigint	YES	false	None	
uv	bigint	YES	false	None	
-- !result
insert into agg_week values
('2026-07-13 01:00:00', 'sh', 1, 10),
('2026-07-15 02:00:00', 'sh', 2, 20),
('2026-07-19 23:59:59', 'bj', 4, 40),
('2026-07-20 00:00:00', 'sh', 8, 80),
('2026-07-26 12:00:00', 'bj', 16, 160),
('2026-08-03 00:00:00', 'sh', 32, 320);
-- result:
-- !result
select partition_name from information_schema.partitions_meta
where table_name = 'agg_week' order by partition_name;
-- result:
$shadow_automatic_partition
p20260713000000
p20260720000000
p20260803000000
-- !result
select * from agg_week order by event_day, city;
-- result:
2026-07-13 01:00:00	sh	1	10
2026-07-15 02:00:00	sh	2	20
2026-07-19 23:59:59	bj	4	40
2026-07-20 00:00:00	sh	8	80
2026-07-26 12:00:00	bj	16	160
2026-08-03 00:00:00	sh	32	320
-- !result
insert into agg_week values
('2026-07-13 01:00:00', 'sh', 1, 5),
('2026-07-15 02:00:00', 'sh', 3, 30),
('2026-08-03 00:00:00', 'sh', 32, 160);
-- result:
-- !result
select event_day, city, pv, uv from agg_week order by event_day, city;
-- result:
2026-07-13 01:00:00	sh	2	10
2026-07-15 02:00:00	sh	5	30
2026-07-19 23:59:59	bj	4	40
2026-07-20 00:00:00	sh	8	80
2026-07-26 12:00:00	bj	16	160
2026-08-03 00:00:00	sh	64	320
-- !result
select count(*), sum(pv), max(uv) from agg_week;
-- result:
6	99	320
-- !result
-- name: test_agg_table_generated_partition_column_ddl @native
create table agg_week_ddl (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
-- !result
show create table agg_week_ddl;
-- result:
agg_week_ddl	CREATE TABLE `agg_week_ddl` (
  `event_day` datetime NOT NULL COMMENT "",
  `city` varchar(64) NOT NULL COMMENT "",
  `pv` bigint(20) SUM NULL COMMENT ""
) ENGINE=OLAP 
AGGREGATE KEY(`event_day`, `city`)
PARTITION BY (date_trunc('week', event_day))
DISTRIBUTED BY HASH(`city`) BUCKETS 3 
PROPERTIES (
"compression" = "LZ4",
"fast_schema_evolution" = "true",
"replicated_storage" = "true",
"replication_num" = "1"
);
-- !result
-- name: test_agg_table_generated_partition_column_prune
create table agg_week_prune (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
-- !result
insert into agg_week_prune values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-20 01:00:00', 'sh', 2),
('2026-07-27 01:00:00', 'sh', 4),
('2026-08-03 01:00:00', 'sh', 8);
-- result:
-- !result
select event_day, pv from agg_week_prune where event_day >= '2026-07-20' and event_day < '2026-07-27' order by event_day;
-- result:
2026-07-20 01:00:00	2
-- !result
select sum(pv) from agg_week_prune where event_day >= '2026-07-27';
-- result:
12
-- !result
select sum(pv) from agg_week_prune where event_day = '2026-07-20 01:00:00';
-- result:
2
-- !result
select city, sum(pv) from agg_week_prune where event_day >= '2026-07-20' group by city order by city;
-- result:
sh	14
-- !result
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
-- result:
-- !result
insert into agg_week_alter values ('2026-07-13 01:00:00', 'sh', 'app', 1), ('2026-07-20 01:00:00', 'sh', 'web', 2);
-- result:
-- !result
alter table agg_week_alter add column clicks bigint sum default "0";
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
desc agg_week_alter;
-- result:
event_day	datetime	NO	true	None	
city	varchar(64)	NO	true	None	
channel	varchar(64)	NO	true	None	
pv	bigint	YES	false	None	
clicks	bigint	YES	false	0	
-- !result
insert into agg_week_alter values ('2026-07-13 01:00:00', 'sh', 'app', 1, 7);
-- result:
-- !result
select event_day, city, channel, pv, clicks from agg_week_alter order by event_day, city, channel;
-- result:
2026-07-13 01:00:00	sh	app	2	7
2026-07-20 01:00:00	sh	web	2	0
-- !result
alter table agg_week_alter add rollup r_city (city, pv);
-- result:
-- !result
function: wait_alter_table_finish("ROLLUP", 8)
-- result:
None
-- !result
select city, sum(pv) from agg_week_alter group by city order by city;
-- result:
sh	4
-- !result
alter table agg_week_alter drop column channel;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select event_day, city, pv, clicks from agg_week_alter order by event_day, city;
-- result:
2026-07-13 01:00:00	sh	2	7
2026-07-20 01:00:00	sh	2	0
-- !result
-- name: test_agg_table_multi_expression_partition
create table agg_multi_expr (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum
) aggregate key(event_day, city)
partition by (city, date_trunc('day', event_day))
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
-- !result
insert into agg_multi_expr values
('2026-07-13 01:00:00', 'sh', 1),
('2026-07-13 02:00:00', 'sh', 2),
('2026-07-13 03:00:00', 'bj', 4),
('2026-07-14 03:00:00', 'bj', 8);
-- result:
-- !result
select partition_name from information_schema.partitions_meta
where table_name = 'agg_multi_expr' order by partition_name;
-- result:
$shadow_automatic_partition
pbj_20260713000000
pbj_20260714000000
psh_20260713000000
-- !result
select event_day, city, pv from agg_multi_expr order by event_day, city;
-- result:
2026-07-13 01:00:00	sh	1
2026-07-13 02:00:00	sh	2
2026-07-13 03:00:00	bj	4
2026-07-14 03:00:00	bj	8
-- !result
-- name: test_unique_table_generated_partition_column
create table uniq_week (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint
) unique key(event_day, city)
partition by date_trunc('week', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
-- !result
insert into uniq_week values ('2026-07-13 01:00:00', 'sh', 1), ('2026-07-20 01:00:00', 'sh', 2);
-- result:
-- !result
insert into uniq_week values ('2026-07-13 01:00:00', 'sh', 100);
-- result:
-- !result
select event_day, city, pv from uniq_week order by event_day, city;
-- result:
2026-07-13 01:00:00	sh	100
2026-07-20 01:00:00	sh	2
-- !result
-- name: test_agg_table_generated_partition_column_invalid
create table agg_bad_expr (
  event_day datetime not null,
  city varchar(64) not null,
  last_day datetime max,
  pv bigint sum
) aggregate key(event_day, city)
partition by date_trunc('week', last_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: The partition expr should base on key column.')
-- !result
create table agg_user_gencol (
  event_day datetime not null,
  city varchar(64) not null,
  pv bigint sum,
  week_start datetime null as date_trunc('week', event_day)
) aggregate key(event_day, city)
partition by date_trunc('day', event_day)
distributed by hash(city) buckets 3
properties("replication_num" = "1");
-- result:
E: (1064, 'Getting analyzing error. Detail message: Generated Column does not support AGG table.')
-- !result