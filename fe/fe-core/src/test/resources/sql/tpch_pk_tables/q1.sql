WITH cte0 as (
WITH
cte11 as (
select  
	l_orderkey,	
	l_suppkey,
	l_partkey,
	(case when l_returnflag = 'A' then l_tax else NULL end) l_tax,
	(case when l_linestatus = 'B' then l_linenumber else NULL end) l_linenumber,
	(case when l_shipmode = 'ABC' then l_quantity else NULL end) l_quantity
from lineitem0
),
	
cte1 as (
select 
       coalesce(l_orderkey, -1) as orderkey,
       coalesce(l_suppkey, -1) as suppkey,
       coalesce(l_partkey, -1) as partkey, 
       l_tax as tax,
       l_linenumber as linenumber,
       l_quantity as quantity
from cte11
),

cte22 as (
select 
ps_partkey, 
ps_suppkey, 
ps_supplycost,
ps_availqty,
(case when ps_comment = 'A' then ps_supplycost else NULL END) supplycost_a,
(case when ps_comment = 'B' then ps_supplycost else NULL END) supplycost_b,
(case when ps_comment = 'AA' then ps_availqty else NULL END) availqty_a,
(case when ps_comment = 'BB' then ps_availqty else NULL END) availqty_b
from partsupp0
),

cte2 as (
select ps_suppkey as suppkey,
sum(ps_supplycost) as supplycost,
count(ps_availqty) as availqty,
sum(supplycost_a) as supplycost_a,
sum(supplycost_b) as supplycost_b,
count(availqty_a) as availqty_a,
count(availqty_b) as availqty_b
from cte22
group by ps_suppkey
),

cte33 as (
select 
ps_partkey, 
ps_suppkey, 
ps_supplycost,
ps_availqty,
(case when ps_comment = 'A' then ps_supplycost else NULL END) supplycost_a,
(case when ps_comment = 'B' then ps_supplycost else NULL END) supplycost_b,
(case when ps_comment = 'AA' then ps_availqty else NULL END) availqty_a,
(case when ps_comment = 'BB' then ps_availqty else NULL END) availqty_b
from partsupp
),

cte3 as (
select ps_partkey as partkey,
sum(ps_supplycost) as supplycost,
count(ps_availqty) as availqty,
sum(supplycost_a) as supplycost_a,
sum(supplycost_b) as supplycost_b,
count(availqty_a) as availqty_a,
count(availqty_b) as availqty_b
from cte33
group by ps_partkey
),

cte4 as (
select
cte3.partkey cte3_partkey,
cte3.supplycost cte3_supplycost,
cte3.availqty cte3_availqty,
cte3.supplycost_a cte3_supplycost_a,
cte3.availqty_a cte3_availqty_a,
cte2.suppkey cte2_suppkey,
cte2.supplycost cte2_supplycost,
cte2.availqty cte2_availqty,
cte2.supplycost_b cte2_supplycost_b,
cte2.availqty_b cte2_availqty_b,
cte1.tax cte1_tax,
cte1.linenumber cte1_linenumber,
cte1.quantity cte1_quantity,
lineitem.l_partkey base_partkey,
lineitem.l_suppkey base_suppkey,
lineitem.l_orderkey base_orderkey,
lineitem.l_tax base_tax,
lineitem.l_quantity base_quantity,
lineitem.l_linenumber base_linenumber
from 
lineitem left join cte1 on lineitem.l_orderkey = cte1.orderkey and lineitem.l_partkey = cte1.partkey and lineitem.l_suppkey = cte1.suppkey
	 left join cte2 on lineitem.l_suppkey = cte2.suppkey
	 left join cte3 on lineitem.l_partkey = cte3.partkey
),

cte5 as (
select
base_partkey,
base_suppkey,
base_orderkey,
coalesce(cte3_supplycost_a,0) cte3_supplycost_a,
coalesce(cte2_supplycost_b,0) cte2_supplycost_b,
coalesce(cte3_availqty_a,0) cte3_availqty_a,
coalesce(cte2_availqty_b,0) cte2_availqty_b,
coalesce(cte1_tax,0) cte1_tax,
coalesce(cte1_linenumber,0) cte1_linenumber,
coalesce(cte1_quantity,0) cte1_quantity,
base_tax,
base_quantity,
base_linenumber
from cte4
)

select  
	base_partkey partkey,
	base_suppkey suppkey,
	base_orderkey orderkey,
	(base_tax+cte1_tax+cte2_supplycost_b+cte3_supplycost_a) as tax,
	(base_quantity+cte1_quantity+cte2_availqty_b+cte3_availqty_a) as quantity,
	(base_linenumber+cte1_linenumber+cte2_availqty_b+cte3_availqty_a) as linenumber
from cte4     
)
update lineitem set 
	l_tax = cte0.tax,
	l_quantity = cte0.quantity,
	l_linenumber = cte0.linenumber
from cte0
where 
   lineitem.l_orderkey = cte0.orderkey and 
   lineitem.l_partkey = cte0.partkey and 
   lineitem.l_suppkey = cte0.suppkey
