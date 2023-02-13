-- customer_order_mv (used to match query13)
create materialized view customer_order_mv
distributed by hash(c_custkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey, c_name, c_address, c_nationkey, c_phone, c_mktsegment, c_comment, c_acctbal,
              o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderkey,
              extract(year from o_orderdate) as o_orderyear
   from
       customer
           left outer join
       orders
       on orders.o_custkey=customer.c_custkey;