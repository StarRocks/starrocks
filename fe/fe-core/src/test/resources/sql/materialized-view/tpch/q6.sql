[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[result]
AGGREGATE ([GLOBAL] aggregate [{116: sum=sum(116: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{116: sum=sum(97: revenue)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv3] columns[94: l_shipdate, 95: l_discount, 96: l_quantity, 97: revenue] predicate[95: l_discount >= 0.02 AND 95: l_discount <= 0.04 AND 96: l_quantity < 24 AND 94: l_shipdate >= 1995-01-01 AND 94: l_shipdate < 1996-01-01])
[end]

