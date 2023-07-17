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
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(42: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[39: l_shipdate, 40: l_discount, 41: l_quantity, 42: revenue] predicate[40: l_discount >= 0.02 AND 40: l_discount <= 0.04 AND 41: l_quantity < 24.0 AND 39: l_shipdate >= 1995-01-01 AND 39: l_shipdate < 1996-01-01])
[end]

