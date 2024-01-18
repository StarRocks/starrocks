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
        AGGREGATE ([LOCAL] aggregate [{28: sum=sum(26: case), 29: sum=sum(27: expr)}] group by [[]] having [null]
<<<<<<< HEAD
            SCAN (mv[lineitem_mv] columns[105: l_shipdate, 119: p_type, 123: l_saleprice] predicate[105: l_shipdate >= 1997-02-01 AND 105: l_shipdate < 1997-03-01 AND 105: l_shipdate >= 1997-01-01 AND 105: l_shipdate < 1998-01-01])
=======
            SCAN (mv[lineitem_mv] columns[57: l_shipdate, 71: p_type, 75: l_saleprice] predicate[57: l_shipdate >= 1997-02-01 AND 57: l_shipdate < 1997-03-01])
>>>>>>> 2.5.18
[end]

