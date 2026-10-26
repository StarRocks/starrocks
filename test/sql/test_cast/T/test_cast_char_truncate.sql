-- name: test_cast_char_truncate
-- Test Point: Explicit CHAR casts truncate; assignment casts preserve length errors and pruning retains matching rows.
-- Method: Compare constant/column results, rejected INSERT/UPDATE statements, and list-partition query results.
-- Scope: CHAR cast evaluation, assignment conversion, and ListPartitionPruner.
-- Related: https://github.com/StarRocks/starrocks/pull/75909
select cast(1775580223839 as char(10));
select cast(cast(1775580223839 as char(10)) as decimal);
select cast('hello world' as char(5));
select cast(1775580223839 as varchar(10));
select cast('hello world' as char);
create database char_cast_${uuid0};
use char_cast_${uuid0};
set enable_insert_strict = true;
set insert_max_filter_ratio = 0;
create table src (id int, n bigint, s varchar(64)) distributed by hash(id) buckets 1 properties ('replication_num'='1');
insert into src values (1, 1775580223839, 'hello world'), (2, 1775580223839, '中😀文abc');
select id, cast(n as char(10)), cast(cast(n as varchar(10)) as char(10)), cast(s as char(2)) from src order by id;
create table dst (id int, c char(10)) primary key(id) distributed by hash(id) buckets 1 properties ('replication_num'='1');
insert into dst values (1, 1775580223839);
insert into dst select id, n from src;
select count(*) from dst;
insert into dst select id, cast(n as char(10)) from src;
update dst set c = 1775580223839 where id > 0;
select * from dst order by id;
create table parts (s varchar(64), id int) partition by list(s) (partition p1 values in ('hello world'), partition p2 values in ('hi'), partition p3 values in ('hello again')) distributed by hash(s) buckets 1 properties ('replication_num'='1');
insert into parts values ('hello world', 1), ('hi', 2), ('hello again', 3);
select id from parts where cast(s as char(5)) = 'hello' order by id;
select id from parts where cast(s as char(5)) in ('hello', 'hi') order by id;
select id from parts where cast(s as char(5)) < 'hi' order by id;
select id from parts where cast(s as char(5)) = 'hello' and s = 'hello world' order by id;
select id from parts where cast(s as char(5)) = 'hello' or s = 'hi' order by id;
drop database char_cast_${uuid0} force;
