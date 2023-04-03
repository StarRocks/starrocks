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
AGGREGATE ([GLOBAL] aggregate [{19: max=max(18: sum)}] group by [[]] having [null]
    AGGREGATE ([GLOBAL] aggregate [{110: sum=sum(58: sum_disc_price)}] group by [[49: l_suppkey]] having [null]
        SCAN (mv[lineitem_agg_mv2] columns[49: l_suppkey, 50: l_shipdate, 58: sum_disc_price] predicate[50: l_shipdate >= 1995-07-01 AND 50: l_shipdate < 1995-10-01 AND 50: l_shipdate >= 1995-01-01 AND 50: l_shipdate < 1996-01-01])
[end]

