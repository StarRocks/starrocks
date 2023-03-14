-- partsupp_mv
create materialized view partsupp_mv
distributed by hash(ps_partkey, ps_suppkey) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              n_name,
              p_mfgr,p_size,p_type,
              ps_partkey, ps_suppkey,ps_supplycost,
              r_name,
              s_acctbal,s_address,s_comment,s_name,s_nationkey,s_phone,
              ps_supplycost * ps_availqty as ps_partvalue
   from
              partsupp
                  inner join supplier
                  inner join part
                  inner join nation
                  inner join region
   where
                  partsupp.ps_suppkey = supplier.s_suppkey
     and partsupp.ps_partkey=part.p_partkey
     and supplier.s_nationkey=nation.n_nationkey
     and nation.n_regionkey=region.r_regionkey;

-- lineitem_mv
create materialized view lineitem_mv
distributed by hash(l_shipdate, l_orderkey, l_linenumber) buckets 96
partition by l_shipdate
refresh manual
properties (
    "replication_num" = "1",
    "partition_refresh_number" = "1"
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
              lineitem
                  inner join partsupp
                  inner join orders
                  inner join supplier
                  inner join part
                  inner join customer
                  inner join nation as s_nation
                  inner join nation as c_nation
                  inner join region as s_region
                  inner join region as c_region
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