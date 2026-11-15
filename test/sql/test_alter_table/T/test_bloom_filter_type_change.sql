-- name: test_bloom_filter_type_change
-- Test Point: Inherited plain bloom filters reject unwritable types on primary and duplicate tables.
-- Method: Check the exact DDL error, absence of alter jobs, unchanged data and type, then write after
--         rejection and complete supported conversion, filter removal, and DOUBLE conversion.
-- Scope: SchemaChangeHandler plain bloom filter type validation.
-- Related: https://github.com/StarRocks/StarRocksTest/issues/12570

create database bf_type_${uuid0};
use bf_type_${uuid0};

create table dup_bf (k bigint not null, x varchar(16)) duplicate key(k)
distributed by hash(k) buckets 1
properties ('replication_num'='1', 'bloom_filter_columns'='x');
insert into dup_bf values (1, '7');
alter table dup_bf modify column x double;
show alter table column where TableName = 'dup_bf';
select typeof(x) from dup_bf limit 1;
select k, cast(x as bigint) from dup_bf order by k;
insert into dup_bf values (2, '8');
alter table dup_bf modify column x bigint;
function: wait_alter_table_finish()
select typeof(x) from dup_bf limit 1;
select k, cast(x as bigint) from dup_bf order by k;
alter table dup_bf set ('bloom_filter_columns'='');
function: wait_alter_table_finish()
alter table dup_bf modify column x double;
function: wait_alter_table_finish()
select typeof(x) from dup_bf limit 1;
insert into dup_bf values (3, 9.0);
select k, cast(x as bigint) from dup_bf order by k;

create table pk_bf (k bigint not null, x varchar(16)) primary key(k)
distributed by hash(k) buckets 1
properties ('replication_num'='1', 'bloom_filter_columns'='x');
insert into pk_bf values (1, '11');
alter table pk_bf modify column x double;
show alter table column where TableName = 'pk_bf';
select typeof(x) from pk_bf limit 1;
select k, cast(x as bigint) from pk_bf order by k;
insert into pk_bf values (2, '12');
alter table pk_bf modify column x bigint;
function: wait_alter_table_finish()
select typeof(x) from pk_bf limit 1;
select k, cast(x as bigint) from pk_bf order by k;
alter table pk_bf set ('bloom_filter_columns'='');
function: wait_alter_table_finish()
alter table pk_bf modify column x double;
function: wait_alter_table_finish()
select typeof(x) from pk_bf limit 1;
insert into pk_bf values (3, 13.0);
select k, cast(x as bigint) from pk_bf order by k;

drop database bf_type_${uuid0} force;
