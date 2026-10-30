-- name: test_bloom_filter_type_change
create database bf_type_${uuid0};
-- result:
-- !result
use bf_type_${uuid0};
-- result:
-- !result
create table dup_bf (k bigint not null, x varchar(16)) duplicate key(k)
distributed by hash(k) buckets 1
properties ('replication_num'='1', 'bloom_filter_columns'='x');
-- result:
-- !result
insert into dup_bf values (1, '7');
-- result:
-- !result
alter table dup_bf modify column x double;
-- result:
[REGEX].*Invalid bloom filter column 'x': unsupported type DOUBLE.*
-- !result
show alter table column where TableName = 'dup_bf';
-- result:
-- !result
select typeof(x) from dup_bf limit 1;
-- result:
varchar
-- !result
select k, cast(x as bigint) from dup_bf order by k;
-- result:
1	7
-- !result
insert into dup_bf values (2, '8');
-- result:
-- !result
alter table dup_bf modify column x bigint;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select typeof(x) from dup_bf limit 1;
-- result:
bigint
-- !result
select k, cast(x as bigint) from dup_bf order by k;
-- result:
1	7
2	8
-- !result
alter table dup_bf set ('bloom_filter_columns'='');
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
alter table dup_bf modify column x double;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select typeof(x) from dup_bf limit 1;
-- result:
double
-- !result
insert into dup_bf values (3, 9.0);
-- result:
-- !result
select k, cast(x as bigint) from dup_bf order by k;
-- result:
1	7
2	8
3	9
-- !result
create table pk_bf (k bigint not null, x varchar(16)) primary key(k)
distributed by hash(k) buckets 1
properties ('replication_num'='1', 'bloom_filter_columns'='x');
-- result:
-- !result
insert into pk_bf values (1, '11');
-- result:
-- !result
alter table pk_bf modify column x double;
-- result:
[REGEX].*Invalid bloom filter column 'x': unsupported type DOUBLE.*
-- !result
show alter table column where TableName = 'pk_bf';
-- result:
-- !result
select typeof(x) from pk_bf limit 1;
-- result:
varchar
-- !result
select k, cast(x as bigint) from pk_bf order by k;
-- result:
1	11
-- !result
insert into pk_bf values (2, '12');
-- result:
-- !result
alter table pk_bf modify column x bigint;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select typeof(x) from pk_bf limit 1;
-- result:
bigint
-- !result
select k, cast(x as bigint) from pk_bf order by k;
-- result:
1	11
2	12
-- !result
alter table pk_bf set ('bloom_filter_columns'='');
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
alter table pk_bf modify column x double;
-- result:
-- !result
function: wait_alter_table_finish()
-- result:
None
-- !result
select typeof(x) from pk_bf limit 1;
-- result:
double
-- !result
insert into pk_bf values (3, 13.0);
-- result:
-- !result
select k, cast(x as bigint) from pk_bf order by k;
-- result:
1	11
2	12
3	13
-- !result
drop database bf_type_${uuid0} force;
-- result:
-- !result
