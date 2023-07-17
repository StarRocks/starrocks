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
<<<<<<< HEAD
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(54: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[51: l_shipdate, 52: l_discount, 53: l_quantity, 54: revenue] predicate[52: l_discount >= 0.02 AND 52: l_discount <= 0.04 AND 53: l_quantity < 24 AND 51: l_shipdate >= 1995-01-01 AND 51: l_shipdate < 1996-01-01])
=======
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(42: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[39: l_shipdate, 40: l_discount, 41: l_quantity, 42: revenue] predicate[40: l_discount >= 0.02 AND 40: l_discount <= 0.04 AND 41: l_quantity < 24.0 AND 39: l_shipdate >= 1995-01-01 AND 39: l_shipdate < 1996-01-01])
>>>>>>> 9408b7a6e9 ([BugFix] Decimal cast to string on FE (#27235))
[end]

