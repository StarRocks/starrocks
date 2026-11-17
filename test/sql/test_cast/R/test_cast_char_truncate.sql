-- name: test_cast_char_truncate
select cast(1775580223839 as char(10));
-- result:
1775580223
-- !result
select cast(cast(1775580223839 as char(10)) as decimal);
-- result:
1775580223
-- !result
select cast('hello world' as char(5));
-- result:
hello
-- !result
select cast(1775580223839 as varchar(10));
-- result:
1775580223839
-- !result
select cast('hello world' as char);
-- result:
hello world
-- !result
create database char_cast_${uuid0};
-- result:
-- !result
use char_cast_${uuid0};
-- result:
-- !result
set enable_insert_strict = true;
-- result:
-- !result
set insert_max_filter_ratio = 0;
-- result:
-- !result
create table src (id int, n bigint, s varchar(64)) distributed by hash(id) buckets 1 properties ('replication_num'='1');
-- result:
-- !result
insert into src values (1, 1775580223839, 'hello world'), (2, 1775580223839, '中😀文abc');
-- result:
-- !result
select id, cast(n as char(10)), cast(cast(n as varchar(10)) as char(10)), cast(s as char(2)) from src order by id;
-- result:
1	1775580223	1775580223	he
2	1775580223	1775580223	中😀
-- !result
create table dst (id int, c char(10)) primary key(id) distributed by hash(id) buckets 1 properties ('replication_num'='1');
-- result:
-- !result
insert into dst values (1, 1775580223839);
-- result:
[REGEX].*Insert has filtered data.*
-- !result
insert into dst select id, n from src;
-- result:
[REGEX].*Insert has filtered data.*
-- !result
select count(*) from dst;
-- result:
0
-- !result
insert into dst select id, cast(n as char(10)) from src;
-- result:
-- !result
update dst set c = 1775580223839 where id > 0;
-- result:
[REGEX].*Insert has filtered data.*
-- !result
select * from dst order by id;
-- result:
1	1775580223
2	1775580223
-- !result
create table parts (s varchar(64), id int) partition by list(s) (partition p1 values in ('hello world'), partition p2 values in ('hi'), partition p3 values in ('hello again')) distributed by hash(s) buckets 1 properties ('replication_num'='1');
-- result:
-- !result
insert into parts values ('hello world', 1), ('hi', 2), ('hello again', 3);
-- result:
-- !result
select id from parts where cast(s as char(5)) = 'hello' order by id;
-- result:
1
3
-- !result
select id from parts where cast(s as char(5)) in ('hello', 'hi') order by id;
-- result:
1
2
3
-- !result
select id from parts where cast(s as char(5)) < 'hi' order by id;
-- result:
1
3
-- !result
select id from parts where cast(s as char(5)) = 'hello' and s = 'hello world' order by id;
-- result:
1
-- !result
select id from parts where cast(s as char(5)) = 'hello' or s = 'hi' order by id;
-- result:
1
2
3
-- !result
drop database char_cast_${uuid0} force;
-- result:
-- !result
