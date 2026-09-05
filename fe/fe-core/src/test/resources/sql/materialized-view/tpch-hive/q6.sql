[sql]
select
    sum(l_extendedprice * l_discount) as revenue
from
    hive0.tpch.lineitem
where
        l_shipdate >= date '1995-01-01'
  and l_shipdate < date '1996-01-01'
  and l_discount between 0.02 and 0.04
  and l_quantity < 24 ;
[result]
AGGREGATE ([GLOBAL] aggregate [{95: sum=sum(95: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{95: sum=sum(94: revenue)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv3] columns[91: l_shipdate, 92: l_discount, 93: l_quantity, 94: revenue] predicate[91: l_shipdate >= 1995-01-01 AND 91: l_shipdate < 1996-01-01 AND 93: l_quantity < 24.00 AND 92: l_discount >= 0.02 AND 92: l_discount <= 0.04])
[end]

