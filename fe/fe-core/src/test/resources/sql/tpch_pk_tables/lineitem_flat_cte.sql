with lineitem_flat as (
with nationCte as (
select
n_nationkey,
n_name,
n_regionkey,
n_comment,
r_name,
r_comment
from
nation inner join region on nation.n_regionkey = region.r_regionkey
),

customerCte as (
select
c_custkey,
c_name,
c_address,
c_nationkey,
c_phone,
c_acctbal,
c_mktsegment,
c_comment,
n_nationkey,
n_name,
n_regionkey,
n_comment,
r_name,
r_comment
from
   customer inner join nationCte on customer.c_nationkey = nationCte.n_nationkey
),

supplierCte as (
select
s_suppkey,
s_name,
s_address,
s_nationkey,
s_phone,
s_acctbal,
s_comment,
n_nationkey,
n_name,
n_regionkey,
n_comment,
r_name,
r_comment
from
   supplier inner join nationCte on supplier.s_nationkey = nationCte.n_nationkey
),

ordersCte as (
select
o_orderkey,
o_custkey,
o_orderstatus,
o_totalprice,
o_orderdate,
o_orderpriority,
o_clerk,
o_shippriority,
o_comment,
c_name,
c_address,
c_nationkey,
c_phone,
c_acctbal,
c_mktsegment,
c_comment,
c_custkey,
n_nationkey,
n_name,
n_regionkey,
n_comment,
r_name,
r_comment
from orders inner join customerCte on orders.o_custkey = customerCte.c_custkey
)

select
l_orderkey,
l_partkey,
l_suppkey,
l_linenumber,
l_quantity,
l_extendedprice,
l_discount,
l_tax,
l_returnflag,
l_linestatus,
l_shipdate,
l_commitdate,
l_receiptdate,
l_shipinstruct,
l_shipmode,
l_comment,
ps_availqty,
ps_supplycost,
ps_comment,
p_name,
p_mfgr,
p_brand,
p_type,
p_size,
p_container,
p_retailprice,
p_comment,
s_name,
s_address,
s_nationkey,
s_phone,
s_acctbal,
s_comment,
supplierCte.n_name as sn_name,
supplierCte.n_comment as sn_comment,
supplierCte.r_name as sr_name,
supplierCte.r_comment sr_comment,
o_custkey,
o_orderstatus,
o_totalprice,
o_orderdate,
o_orderpriority,
o_clerk,
o_shippriority,
o_comment,
c_name,
c_address,
c_nationkey,
c_phone,
c_acctbal,
c_mktsegment,
c_comment,
ordersCte.n_name as cn_name,
ordersCte.n_comment as cn_comment,
ordersCte.r_name as cr_name,
ordersCte.r_comment as cr_comment
from
    lineitem inner join partsupp on lineitem.l_partkey = partsupp.ps_partkey and lineitem.l_suppkey = partsupp.ps_suppkey
    inner join part on lineitem.l_partkey = part.p_partkey
    inner join supplierCte on lineitem.l_suppkey = supplierCte.s_suppkey
    inner join ordersCte on lineitem.l_orderkey = ordersCte.o_orderkey
)
