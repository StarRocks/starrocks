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
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(40: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[37: l_shipdate, 38: l_discount, 39: l_quantity, 40: revenue] predicate[38: l_discount >= 0.02 AND 38: l_discount <= 0.04 AND 39: l_quantity < 24 AND 37: l_shipdate >= 1995-01-01 AND 37: l_shipdate < 1996-01-01])
=======
AGGREGATE ([GLOBAL] aggregate [{100: sum=sum(33: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[30: l_shipdate, 31: l_discount, 32: l_quantity, 33: revenue] predicate[32: l_quantity < 24.00 AND 31: l_discount >= 0.02 AND 31: l_discount <= 0.04 AND 30: l_shipdate >= 1995-01-01 AND 30: l_shipdate < 1996-01-01])
>>>>>>> 2.5.18
[end]

