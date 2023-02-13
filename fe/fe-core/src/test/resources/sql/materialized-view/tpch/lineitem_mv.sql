-- lineitem_mv
create materialized view lineitem_mv
distributed by hash(o_orderdate, l_orderkey) buckets 20
refresh manual
properties (
    "replication_num" = "1"
)
as select
        l_orderkey, l_partkey, l_linenumber, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate,
            l_shipinstruct, l_shipmode, l_comment, l_shipyear, l_receiptdelayed, l_shipdelayed, l_suppkey, l_discount,
        o_orderkey, o_custkey, o_orderstatus, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment, o_orderyear,
        s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment, s_suppkey,
        p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment,
        ps_partkey, ps_availqty, ps_comment, ps_suppkey, ps_supplycost, ps_partvalue,
        c_custkey, c_name, c_address, c_nationkey, c_phone, c_mktsegment, c_comment, c_acctbal,
        s_nation.n_nationkey as n_nationkey1, s_nation.n_name as n_name1, s_nation.n_regionkey as n_regionkey1, s_nation.n_comment as n_comment1,
        c_nation.n_name as n_name2, c_nation.n_regionkey as n_regionkey2, c_nation.n_comment as n_comment2, c_nation.n_nationkey as n_nationkey2,
        s_region.r_regionkey as r_regionkey1, s_region.r_name as r_name1, s_region.r_comment as r_comment1,
        c_region.r_regionkey as r_regionkey2, c_region.r_name as r_name2, c_region.r_comment as r_comment2
   from
       (select
            lineitem.*,
            extract(year from l_shipdate) as l_shipyear,
            case when l_commitdate < l_receiptdate then 1 else 0 end as l_receiptdelayed,
            case when l_shipdate < l_commitdate then 0 else 1 end as l_shipdelayed,
            l_extendedprice * (1 - l_discount) as l_saleprice,
            l_extendedprice * (1 - l_discount) * l_tax as l_taxprice,
            ps_supplycost * l_quantity as l_supplycost,
            partsupp.*,
            ps_supplycost * ps_availqty as ps_partvalue
        from
            lineitem
                inner join partsupp on l_partkey=ps_partkey and l_suppkey=ps_suppkey) as v_lineitem
       inner join
       (select
            orders.*,
            extract(year from o_orderdate) as o_orderyear
        from
                  orders) as v_orders
       inner join supplier
       inner join part
       inner join customer
       inner join nation as s_nation
       inner join nation as c_nation
       inner join region as s_region
       inner join region as c_region
   where
                  v_lineitem.l_orderkey=v_orders.o_orderkey
     and v_lineitem.l_suppkey=supplier.s_suppkey
     and v_lineitem.ps_partkey=part.p_partkey
     and customer.c_custkey=v_orders.o_custkey
     and supplier.s_nationkey=s_nation.n_nationkey
     and customer.c_nationkey=c_nation.n_nationkey
     and s_region.r_regionkey=s_nation.n_regionkey
     and c_region.r_regionkey=c_nation.n_regionkey
;