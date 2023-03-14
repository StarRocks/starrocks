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
AGGREGATE ([GLOBAL] aggregate [{20: max=max(20: max)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{20: max=max(19: sum)}] group by [[]] having [null]
            AGGREGATE ([GLOBAL] aggregate [{90: sum=sum(90: sum)}] group by [[22: l_suppkey]] having [null]
                EXCHANGE SHUFFLE[22]
                    AGGREGATE ([LOCAL] aggregate [{90: sum=sum(33: sum_disc_price)}] group by [[22: l_suppkey]] having [null]
                        SCAN (mv[lineitem_agg_mv] columns[22: l_suppkey, 23: l_shipdate, 33: sum_disc_price] predicate[23: l_shipdate >= 1995-07-01 AND 23: l_shipdate < 1995-10-01])
[end]

