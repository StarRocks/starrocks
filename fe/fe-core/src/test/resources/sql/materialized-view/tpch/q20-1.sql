[sql]
select
        0.5 * sum(l_quantity)
from
    lineitem
where
  l_shipdate >= date '1993-01-01'
  and l_shipdate < date '1994-01-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{116: sum=sum(116: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{116: sum=sum(26: sum_qty)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv2] columns[24: l_shipdate, 26: sum_qty] predicate[24: l_shipdate >= 1993-01-01 AND 24: l_shipdate < 1994-01-01])
[end]

