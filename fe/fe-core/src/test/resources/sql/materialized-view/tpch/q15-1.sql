[sql]
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
             l_suppkey) b;
[result]
AGGREGATE ([GLOBAL] aggregate [{19: max=max(19: max)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{19: max=max(18: sum)}] group by [[]] having [null]
            AGGREGATE ([GLOBAL] aggregate [{117: sum=sum(117: sum)}] group by [[80: l_suppkey]] having [null]
                EXCHANGE SHUFFLE[80]
                    AGGREGATE ([LOCAL] aggregate [{117: sum=sum(89: sum_disc_price)}] group by [[80: l_suppkey]] having [null]
                        SCAN (mv[lineitem_agg_mv2] columns[80: l_suppkey, 81: l_shipdate, 89: sum_disc_price] predicate[81: l_shipdate >= 1995-07-01 AND 81: l_shipdate < 1995-10-01 AND 81: l_shipdate >= 1995-01-01 AND 81: l_shipdate < 1996-01-01])
[end]

