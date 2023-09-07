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
AGGREGATE ([GLOBAL] aggregate [{100: sum=sum(99: revenue)}] group by [[]] having [null]
    SCAN (mv[lineitem_agg_mv3] columns[96: l_shipdate, 97: l_discount, 98: l_quantity, 99: revenue] predicate[96: l_shipdate >= 1995-01-01 AND 96: l_shipdate < 1996-01-01 AND 98: l_quantity < 24.00 AND 97: l_discount >= 0.02 AND 97: l_discount <= 0.04])
[end]

