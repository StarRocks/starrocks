-- query9
create materialized view lineitem_mv_agg_mv1
 distributed by hash(p_name,
               o_orderyear,
               n_name1) buckets 96
 refresh manual
 properties (
     "replication_num" = "1"
 )
 as select /*+ SET_VAR(query_timeout = 7200) */
               p_name,
               o_orderyear,
               n_name1,
               sum(l_amount) as sum_amount
    from
               lineitem_mv
    group by
        p_name,
        o_orderyear,
        n_name1
;

-- query7
create materialized view lineitem_mv_agg_mv2
 distributed by hash(l_shipdate,
        n_name1,
        n_name2,
        l_shipyear) buckets 96
 refresh manual
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

-- query4
create materialized view query4_mv
 distributed by hash(o_orderdate,
    o_orderpriority) buckets 24
 refresh manual
 properties (
     "replication_num" = "1"
 )
as select
    o_orderdate,
    o_orderpriority,
    count(*) as order_count
from
    orders
where
  exists (
        select
            *
        from
            lineitem
        where
                l_orderkey = o_orderkey
          and l_receiptdate > l_commitdate
    )
group by
    o_orderdate,
    o_orderpriority;

-- query21
create materialized view query21_mv
distributed by hash(s_name,
              o_orderstatus,
              n_name) buckets 24
refresh manual
properties (
    "replication_num" = "1"
)
as select
              s_name,
              o_orderstatus,
              n_name,
              count(*) as cnt_star
   from
       supplier,
       lineitem l1,
       orders,
       nation
   where
                  s_suppkey = l1.l_suppkey
     and o_orderkey = l1.l_orderkey
     and l1.l_receiptdate > l1.l_commitdate
     and exists (
           select
               *
           from
               lineitem l2
           where
                   l2.l_orderkey = l1.l_orderkey
             and l2.l_suppkey <> l1.l_suppkey
       )
     and not exists (
           select
               *
           from
               lineitem l3
           where
                   l3.l_orderkey = l1.l_orderkey
             and l3.l_suppkey <> l1.l_suppkey
             and l3.l_receiptdate > l3.l_commitdate
       )
     and s_nationkey = n_nationkey
   group by s_name,
            o_orderstatus,
            n_name;
