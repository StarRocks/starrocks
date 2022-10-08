drop materialized view mv1;
drop table t0;
drop table t1;

CREATE TABLE `t0` (
  `t0_c0` int(11) NOT NULL COMMENT "",
  `t0_c1` string NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`t0_c0`)
DISTRIBUTED BY HASH(`t0_c0`) BUCKETS 1
PROPERTIES ( "replication_num" = "1", "colocate_with" = "group1");

CREATE TABLE `t1` (
  `t1_c0` int(11) NOT NULL COMMENT "",
  `t1_c1` string NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`t1_c0`)
DISTRIBUTED BY HASH(`t1_c0`) BUCKETS 1
PROPERTIES ( "replication_num" = "1", "colocate_with" = "group1");

insert into t0 values (1, 'star');
insert into t1 values (1, 'star');

-- step 1
create materialized view mv1  
distributed by hash(t0_c0) 
refresh realtime 
as 
select t0_c0, t1_c1
from t0
join t1 on t0_c0 = t1_c0;

explain insert into t0 values (1, 'star');

select * from mv1;

-- no hit
insert into t0 values (0, 'star');
select * from mv1;

-- hit
insert into t0 values (1, 'star');
select * from mv1;

drop materialized view mv1;

-- insert new valuse in t1
insert into t1 values (2, 'b');

-- step 2
-- correct view-update output columns
create materialized view mv2
distributed by hash(t0_c0) 
refresh realtime 
as 
select t0_c0, t0_c1, t1_c0, t1_c1
from t0
join t1 on t0_c0 = t1_c0;

explain insert into t0 values (0, 'a0');
select * from mv2;

-- no hit
insert into t0 values (0, 'a0');
select * from mv2;

-- hit
insert into t0 values (2, 'star');
select * from mv2;

drop materialized view mv2;

-- insert new value
insert into t1 values (3, 'c');

-- step 3
-- support projection and select in MV
create materialized view mv3
distributed by hash(mv3_c0) 
refresh realtime 
as 
select t0_c0 + 1 as mv3_c0, t0_c1, t1_c0, t1_c1
from t0
join t1 on t0_c0 = t1_c0
where t0_c1 = 'star';

explain insert into t0 values (0, 'a0');
select * from mv3;

-- no hit
insert into t0 values (0, 'a0');
select * from mv3;

-- hit
insert into t0 values (3, 'li');
select * from mv3;
drop materialized view mv3;

-- step 4
insert into t1 values (4, 'd');

-- infer primary key
create materialized view mv4
refresh realtime 
as 
select t0_c0, t0_c1
from t0
join t1 on t0_c0 = t1_c0;

explain insert into t0 values (0, 'a0');
select * from mv4;

-- no hit
insert into t0 values (0, 'a0');
select * from mv4;

-- hit
insert into t0 values (4, 'lism');
select * from mv4;
drop materialized view mv4;

-- step 5
-- support aggregation
create materialized view mv5
refresh realtime 
as 
select t0_c0, count(t0_c0) as cnt, sum(t0_c0) as sum
from t0
group by t0_c0;

explain insert into t0 values (0, 'a0');
select * from mv5;

-- no hit
insert into t0 values (0, 'a0');
select * from mv5;

-- hit
insert into t0 values (1, 'a');
select * from mv5;
insert into t0 values (1, 'b');
select * from mv5;
insert into t0 values (1, 'c');
select * from mv5;
insert into t0 values (2, 'a');
select * from mv5;
insert into t0 values (2, 'b');
select * from mv5;
drop materialized view mv5;

-- step 6
-- support aggregation + join
create materialized view mv6
refresh realtime 
as 
select t0_c0, count(t0_c0) as cnt, sum(t0_c0) as sum
from t0
join t1 on t0_c0 = t1_c0
group by t0_c0;

explain insert into t0 values (0, 'a0');
select * from mv6;

-- no hit
insert into t0 values (0, 'a0');
select * from mv6;

-- hit
insert into t0 values (1, 'a');
select * from mv6;
insert into t0 values (1, 'b');
select * from mv6;
insert into t0 values (1, 'c');
select * from mv6;
insert into t0 values (2, 'a');
select * from mv6;
insert into t0 values (2, 'b');
select * from mv6;
drop materialized view mv6;


-- cleanup
drop table t0;
drop table t1;
