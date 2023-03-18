[sql]
select
        0.5 * sum(l_quantity)
from
    lineitem
where
  l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-01-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{107: sum=sum(80: sum_qty)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv2] columns[78: l_shipdate, 80: sum_qty] predicate[78: l_shipdate >= 1993-01-01 AND 78: l_shipdate < 1994-01-01])
[end]

