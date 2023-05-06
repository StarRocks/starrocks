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
            AGGREGATE ([GLOBAL] aggregate [{107: sum=sum(102: sum_disc_price)}] group by [[93: l_suppkey]] having [null]
                SCAN (mv[lineitem_agg_mv2] columns[93: l_suppkey, 94: l_shipdate, 102: sum_disc_price] predicate[94: l_shipdate >= 1995-07-01 AND 94: l_shipdate < 1995-10-01])
[end]

