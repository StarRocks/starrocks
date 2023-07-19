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
        AGGREGATE ([LOCAL] aggregate [{: sum=sum(if(: p_type LIKE PROMO%, : l_saleprice, .)), : sum=sum(: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[: l_shipdate, : p_type, : l_saleprice] predicate[: l_shipdate >= -- AND : l_shipdate < --])
[end]

