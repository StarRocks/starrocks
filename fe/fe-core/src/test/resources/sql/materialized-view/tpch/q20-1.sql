[sql]
select
        0.5 * sum(l_quantity)
from
    lineitem
where
  l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-01-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{106: sum=sum(23: sum_qty)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv1] columns[20: l_shipdate, 23: sum_qty] predicate[20: l_shipdate >= 1993-01-01 AND 20: l_shipdate < 1994-01-01])
[end]

