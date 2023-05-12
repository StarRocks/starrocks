--lineitem_agg_mv1
create materialized view lineitem_agg_mv1
distributed by hash(l_orderkey,
               l_shipdate,
               l_returnflag,
               l_linestatus) buckets 96
refresh deferred manual
properties (
    "replication_num" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_orderkey,
              l_shipdate,
              l_returnflag,
              l_linestatus,
              sum(l_quantity) as sum_qty,
              count(l_quantity) as count_qty,
              sum(l_extendedprice) as sum_base_price,
              count(l_extendedprice) as count_base_price,
              sum(l_discount) as sum_discount,
              count(l_discount) as count_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
              count(*) as count_order
   from
              hive0.tpch.lineitem
   group by
       l_orderkey,
       l_shipdate,
       l_returnflag,
       l_linestatus
;

--lineitem_agg_mv2
create materialized view lineitem_agg_mv2
distributed by hash(l_suppkey, l_shipdate, l_partkey) buckets 96
refresh deferred manual
properties (
    "replication_num" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
              l_suppkey, l_shipdate, l_partkey,
              sum(l_quantity) as sum_qty,
              count(l_quantity) as count_qty,
              sum(l_extendedprice) as sum_base_price,
              count(l_extendedprice) as count_base_price,
              sum(l_discount) as sum_discount,
              count(l_discount) as count_discount,
              sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
              sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge
   from
              hive0.tpch.lineitem
   group by
       l_suppkey, l_shipdate, l_partkey
;

-- partsupp_mv
create materialized view partsupp_mv
distributed by hash(ps_partkey, ps_suppkey) buckets 24
refresh deferred manual
properties (
    "replication_num" = "1",
    "unique_constraints" = "hive0.tpch.part.p_partkey;hive0.tpch.region.r_regionkey",
    "foreign_key_constraints" = "hive0.tpch.partsupp(ps_partkey) references hive0.tpch.part(p_partkey);hive0.tpch.nation(n_regionkey) references hive0.tpch.region(r_regionkey)"
)
as select
              n_name,
              p_mfgr,p_size,p_type,
              ps_partkey, ps_suppkey,ps_supplycost,
              r_name,
              s_acctbal,s_address,s_comment,s_name,s_nationkey,s_phone,
              ps_supplycost * ps_availqty as ps_partvalue
   from
              hive0.tpch.partsupp
              inner join hive0.tpch.supplier
              inner join hive0.tpch.part
              inner join hive0.tpch.nation
              inner join hive0.tpch.region
   where
     partsupp.ps_suppkey = supplier.s_suppkey
     and partsupp.ps_partkey=part.p_partkey
     and supplier.s_nationkey=nation.n_nationkey
     and nation.n_regionkey=region.r_regionkey;


-- lineitem_mv
create materialized view lineitem_mv
distributed by hash(l_shipdate, l_orderkey, l_linenumber) buckets 96
refresh deferred manual
properties (
 "replication_num" = "1",
 "unique_constraints" = "hive0.tpch.partsupp.ps_partkey,ps_suppkey; hive0.tpch.orders.o_orderkey; hive0.tpch.supplier.s_suppkey; hive0.tpch.part.p_partkey; hive0.tpch.customer.c_custkey;
  hive0.tpch.nation.n_nationkey; hive0.tpch.region.r_regionkey;",
 "foreign_key_constraints" = "hive0.tpch.lineitem(l_orderkey) references hive0.tpch.orders(o_orderkey); hive0.tpch.lineitem(l_partkey,l_suppkey) references hive0.tpch.partsupp(ps_partkey, ps_suppkey);
 hive0.tpch.partsupp(ps_partkey) references hive0.tpch.part(p_partkey);hive0.tpch.partsupp(ps_suppkey) references hive0.tpch.supplier(s_suppkey); hive0.tpch.orders(o_custkey) references hive0.tpch.customer(c_custkey);
 hive0.tpch.supplier(s_nationkey) references hive0.tpch.nation(n_nationkey);hive0.tpch.customer(c_nationkey) references hive0.tpch.nation(n_nationkey); hive0.tpch.nation(n_regionkey) references hive0.tpch.region(r_regionkey)"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
           c_address, c_acctbal,c_comment,c_mktsegment,c_name,c_nationkey,c_phone,
           l_commitdate,l_linenumber,l_extendedprice,l_orderkey,l_partkey,l_quantity,l_receiptdate,l_returnflag,l_shipdate,l_shipinstruct,l_shipmode,l_suppkey,
           o_custkey,o_orderdate,o_orderpriority,o_orderstatus,o_shippriority,o_totalprice,
           p_brand,p_container,p_name,p_size,p_type,
           s_name,s_nationkey,
           extract(year from l_shipdate) as l_shipyear,
           l_extendedprice * (1 - l_discount) as l_saleprice,
           l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as l_amount,
           ps_supplycost * l_quantity as l_supplycost,
           extract(year from o_orderdate) as o_orderyear,
           s_nation.n_name as n_name1,
           s_nation.n_regionkey as n_regionkey1,
           c_nation.n_name as n_name2,
           c_nation.n_regionkey as n_regionkey2,
           s_region.r_name as r_name1,
           c_region.r_name as r_name2
from
           hive0.tpch.lineitem
               inner join hive0.tpch.partsupp
               inner join hive0.tpch.orders
               inner join hive0.tpch.supplier
               inner join hive0.tpch.part
               inner join hive0.tpch.customer
               inner join hive0.tpch.nation as s_nation
               inner join hive0.tpch.nation as c_nation
               inner join hive0.tpch.region as s_region
               inner join hive0.tpch.region as c_region
where
               lineitem.l_partkey=partsupp.ps_partkey
  and lineitem.l_suppkey=partsupp.ps_suppkey
  and lineitem.l_orderkey=orders.o_orderkey
  and partsupp.ps_partkey=part.p_partkey
  and partsupp.ps_suppkey=supplier.s_suppkey
  and customer.c_custkey=orders.o_custkey
  and supplier.s_nationkey=s_nation.n_nationkey
  and customer.c_nationkey=c_nation.n_nationkey
  and s_region.r_regionkey=s_nation.n_regionkey
  and c_region.r_regionkey=c_nation.n_regionkey
;

 -- query4_mv
create materialized view query4_mv
distributed by hash(o_orderdate,
 o_orderpriority) buckets 24
refresh deferred manual
properties (
  "replication_num" = "1"
)
as select
 o_orderdate,
 o_orderpriority,
 count(*) as order_count
from
 hive0.tpch.orders
where
exists (
     select
         *
     from
         hive0.tpch.lineitem
     where
             l_orderkey = o_orderkey
       and l_receiptdate > l_commitdate
 )
group by
 o_orderdate,
 o_orderpriority;

 --lineitem_agg_mv3
create materialized view lineitem_agg_mv3
distributed by hash(l_shipdate, l_discount, l_quantity) buckets 24
refresh deferred manual
properties (
 "replication_num" = "1"
)
as select  /*+ SET_VAR(query_timeout = 7200) */
           l_shipdate, l_discount, l_quantity,
           sum(l_extendedprice * l_discount) as revenue
from
           hive0.tpch.lineitem
group by
    l_shipdate, l_discount, l_quantity
;

-- query7
create materialized view lineitem_mv_agg_mv2
 distributed by hash(l_shipdate,
        n_name1,
        n_name2,
        l_shipyear) buckets 96
 refresh deferred manual
 properties (
     "replication_num" = "1"
 )
 as select /*+ SET_VAR(query_timeout = 7200) */
               l_shipdate,
               n_name1,
               n_name2,
               l_shipyear,
               sum(l_saleprice) as sum_saleprice
    from
               lineitem_mv
    group by
        l_shipdate,
        n_name1,
        n_name2,
        l_shipyear
;

-- query21
create materialized view query21_mv
distributed by hash(s_name,
              o_orderstatus,
              n_name) buckets 24
refresh deferred manual
properties (
    "replication_num" = "1"
)
as select
              s_name,
              o_orderstatus,
              n_name,
              count(*) as cnt_star
   from
       hive0.tpch.supplier,
       hive0.tpch.lineitem l1,
       hive0.tpch.orders,
       hive0.tpch.nation
   where
                  s_suppkey = l1.l_suppkey
     and o_orderkey = l1.l_orderkey
     and l1.l_receiptdate > l1.l_commitdate
     and exists (
           select
               *
           from
               hive0.tpch.lineitem l2
           where
                   l2.l_orderkey = l1.l_orderkey
             and l2.l_suppkey <> l1.l_suppkey
       )
     and not exists (
           select
               *
           from
               hive0.tpch.lineitem l3
           where
                   l3.l_orderkey = l1.l_orderkey
             and l3.l_suppkey <> l1.l_suppkey
             and l3.l_receiptdate > l3.l_commitdate
       )
     and s_nationkey = n_nationkey
   group by s_name,
            o_orderstatus,
            n_name;

-- query22
create materialized view customer_agg_mv1
distributed by hash(c_custkey) buckets 24
refresh deferred manual
properties (
    "replication_num" = "1"
)
as select
              c_custkey,
              c_phone,
              c_acctbal,
              substring(c_phone, 1  ,2) as substring_phone,
              count(c_acctbal) as c_count,
              sum(c_acctbal) as c_sum
   from
              hive0.tpch.customer
   group by c_custkey, c_phone, c_acctbal, substring(c_phone, 1  ,2);