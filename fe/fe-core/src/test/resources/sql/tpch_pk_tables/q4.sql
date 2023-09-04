WITH cte0 as (
WITH
cte1 as (
select  
	l_orderkey,	
	l_suppkey,
	l_partkey,
	(case when l_returnflag = 'A' then l_tax else NULL end) l_tax,
	(case when l_linestatus = 'B' then l_linenumber else NULL end) l_linenumber,
	(case when l_shipmode = 'ABC' then l_quantity else NULL end) l_quantity
from lineitem0
),
	
cte11 as (
select 
    avg(l_tax) as tax,
    l_orderkey as orderkey
from cte1
group by l_orderkey
),

cte12 as (
select 
    sum(l_linenumber) as linenumber,
    l_suppkey as suppkey
from cte1
group by l_suppkey
),

cte13 as (
select 
    count(l_quantity) as quantity,
    l_partkey as partkey
from cte1
group by l_partkey
),


cte21 as (
select 
    coalesce(tax,0) tax,
    orderkey
from cte11
),

cte22 as (
select 
    coalesce(linenumber,0) linenumber,
    suppkey
from cte12
),

cte23 as (
select 
    coalesce(quantity,0) quantity,
    partkey
from cte13
),


cte3 as (
select
lineitem.l_tax base_tax,
lineitem.l_linenumber base_linenumber,
lineitem.l_quantity base_quantity,
cte21.tax new_tax,
cte22.linenumber new_linenumber,
cte23.quantity new_quantity,
lineitem.l_partkey base_partkey,
lineitem.l_suppkey base_suppkey,
lineitem.l_orderkey base_orderkey
from 
lineitem left join cte21 on lineitem.l_orderkey = cte21.orderkey
	 left join cte22 on lineitem.l_suppkey = cte22.suppkey
	 left join cte23 on lineitem.l_partkey = cte23.partkey
),

cte4 as (
select
coalesce(new_tax, base_tax) new_tax,
coalesce(new_linenumber, base_linenumber) new_linenumber,
coalesce(new_quantity, base_quantity) new_quantity,
base_orderkey orderkey,
base_partkey partkey,
base_suppkey suppkey
from cte3
),

cte5 as (
select  
new_tax,
new_linenumber,
new_quantity,
orderkey,
partkey,
suppkey
from cte4
)
select 
sum(new_tax) new_tax,
sum(new_linenumber) new_linenumber,
sum(new_quantity) new_quantity,
orderkey,
partkey,
suppkey
from lineitem inner join cte5 on lineitem.l_orderkey = cte5.orderkey and lineitem.l_partkey = cte5.partkey and lineitem.l_suppkey = cte5.suppkey
group by orderkey, partkey, suppkey
)
update lineitem set 
	l_tax = new_tax,
	l_quantity = new_quantity,
	l_linenumber = new_linenumber
from cte0
where 
   lineitem.l_orderkey = cte0.orderkey and 
   lineitem.l_partkey = cte0.partkey and 
   lineitem.l_suppkey = cte0.suppkey

