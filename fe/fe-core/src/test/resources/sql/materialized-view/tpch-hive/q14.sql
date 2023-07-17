[sql]
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    hive0.tpch.lineitem,
    hive0.tpch.part
where
        l_partkey = p_partkey
  and l_shipdate >= date '1997-02-01'
  and l_shipdate < date '1997-03-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{: sum=sum(: sum), : sum=sum(: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
<<<<<<< HEAD
        AGGREGATE ([LOCAL] aggregate [{28: sum=sum(26: case), 29: sum=sum(27: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[55: l_shipdate, 69: p_type, 73: l_saleprice] predicate[55: l_shipdate >= 1997-02-01 AND 55: l_shipdate < 1997-03-01])
[end]
=======
        AGGREGATE ([LOCAL] aggregate [{: sum=sum(if(: p_type LIKE PROMO%, : l_saleprice, .)), : sum=sum(: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[: l_shipdate, : p_type, : l_saleprice] predicate[: l_shipdate >= -- AND : l_shipdate < --])
-- [end]
>>>>>>> 9408b7a6e9 ([BugFix] Decimal cast to string on FE (#27235))

