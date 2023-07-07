[sql]
select
            100.00 * sum(case
                             when p_type like 'PROMO%'
                                 then l_extendedprice * (1 - l_discount)
                             else 0
            end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
    lineitem,
    part
where
        l_partkey = p_partkey
  and l_shipdate >= date '1997-02-01'
  and l_shipdate < date '1997-03-01';
[result]
AGGREGATE ([GLOBAL] aggregate [{28: sum=sum(28: sum), 29: sum=sum(29: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{28: sum=sum(if(115: p_type LIKE PROMO%, 119: l_saleprice, 0)), 29: sum=sum(27: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[101: l_shipdate, 115: p_type, 119: l_saleprice] predicate[101: l_shipdate >= 1997-02-01 AND 101: l_shipdate < 1997-03-01])
[end]

