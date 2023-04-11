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
AGGREGATE ([GLOBAL] aggregate [{: max=max(: max)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{: max=max(: sum)}] group by [[]] having [null]
            AGGREGATE ([GLOBAL] aggregate [{: sum=sum(: sum_disc_price)}] group by [[: l_suppkey]] having [null]
                SCAN (mv[lineitem_agg_mv] columns[: l_suppkey, : l_shipdate, : sum_disc_price] predicate[: l_shipdate >= -- AND : l_shipdate < -- AND : l_shipdate >= -- AND : l_shipdate < --])
[end]

