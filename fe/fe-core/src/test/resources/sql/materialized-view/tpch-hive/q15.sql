[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    hive0.tpch.supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             hive0.tpch.lineitem
         where
                 l_shipdate >= date '1995-07-01'
           and l_shipdate < date '1995-10-01'
         group by
             l_suppkey) a
where
        s_suppkey = supplier_no
  and total_revenue = (
    select
        max(total_revenue)
    from
        (	select
                 l_suppkey as supplier_no,
                 sum(l_extendedprice * (1 - l_discount)) as total_revenue
             from
                 hive0.tpch.lineitem
             where
                     l_shipdate >= date '1995-07-01'
               and l_shipdate < date '1995-10-01'
             group by
                 l_suppkey) b
)
order by
    s_suppkey;
[result]
TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
    TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: s_suppkey = 10: l_suppkey] post-join-predicate [null])
            HIVE SCAN (columns{1,2,3,5} predicate[1: s_suppkey IS NOT NULL])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [25: sum = 44: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{148: sum=sum(148: sum)}] group by [[129: l_suppkey]] having [148: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[129]
                            AGGREGATE ([LOCAL] aggregate [{148: sum=sum(138: sum_disc_price)}] group by [[129: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv2] columns[129: l_suppkey, 130: l_shipdate, 138: sum_disc_price] predicate[130: l_shipdate >= 1995-07-01 AND 130: l_shipdate < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 44: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{44: max=max(44: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{44: max=max(43: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{149: sum=sum(149: sum)}] group by [[129: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[129]
                                                    AGGREGATE ([LOCAL] aggregate [{149: sum=sum(138: sum_disc_price)}] group by [[129: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv2] columns[129: l_suppkey, 130: l_shipdate, 138: sum_disc_price] predicate[130: l_shipdate >= 1995-07-01 AND 130: l_shipdate < 1995-10-01])
[end]

