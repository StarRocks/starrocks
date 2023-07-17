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
AGGREGATE ([GLOBAL] aggregate [{107: sum=sum(107: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{107: sum=sum(101: revenue)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv3] columns[98: l_shipdate, 99: l_discount, 100: l_quantity, 101: revenue] predicate[98: l_shipdate >= 1995-01-01 AND 98: l_shipdate < 1996-01-01 AND 99: l_discount >= 0.02 AND 99: l_discount <= 0.04 AND 100: l_quantity < 24])
=======
        AGGREGATE ([LOCAL] aggregate [{87: sum=sum(81: revenue)}] group by [[]] having [null]
            SCAN (mv[lineitem_agg_mv3] columns[78: l_shipdate, 79: l_discount, 80: l_quantity, 81: revenue] predicate[78: l_shipdate >= 1995-01-01 AND 78: l_shipdate < 1996-01-01 AND 79: l_discount >= 0.02 AND 79: l_discount <= 0.04 AND 80: l_quantity < 24.0])
>>>>>>> 9408b7a6e9 ([BugFix] Decimal cast to string on FE (#27235))
[end]

