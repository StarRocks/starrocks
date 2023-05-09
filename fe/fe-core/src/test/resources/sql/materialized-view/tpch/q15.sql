[sql]
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    (	select
             l_suppkey as supplier_no,
             sum(l_extendedprice * (1 - l_discount)) as total_revenue
         from
             lineitem
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
                 lineitem
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
        INNER JOIN (join-predicate [1: s_suppkey = 12: l_suppkey] post-join-predicate [null])
            SCAN (table[supplier] columns[1: s_suppkey, 2: s_name, 3: s_address, 5: s_phone] predicate[null])
            EXCHANGE SHUFFLE[12]
                INNER JOIN (join-predicate [25: sum = 44: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{148: sum=sum(148: sum)}] group by [[134: l_suppkey]] having [148: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[134]
                            AGGREGATE ([LOCAL] aggregate [{148: sum=sum(143: sum_disc_price)}] group by [[134: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv2] columns[134: l_suppkey, 135: l_shipdate, 143: sum_disc_price] predicate[135: l_shipdate >= 1995-07-01 AND 135: l_shipdate < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 44: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{44: max=max(44: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{44: max=max(43: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{149: sum=sum(149: sum)}] group by [[134: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[134]
                                                    AGGREGATE ([LOCAL] aggregate [{149: sum=sum(143: sum_disc_price)}] group by [[134: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv2] columns[134: l_suppkey, 135: l_shipdate, 143: sum_disc_price] predicate[135: l_shipdate >= 1995-07-01 AND 135: l_shipdate < 1995-10-01])
[end]

