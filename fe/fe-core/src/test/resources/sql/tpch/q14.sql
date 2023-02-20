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
AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{30: sum=sum(if(22: P_TYPE LIKE PROMO%, 34: multiply, 0.0)), 31: sum=sum(29: expr)}] group by [[]] having [null]
            INNER JOIN (join-predicate [18: P_PARTKEY = 2: L_PARTKEY] post-join-predicate [null])
                SCAN (columns[18: P_PARTKEY, 22: P_TYPE] predicate[null])
                EXCHANGE SHUFFLE[2]
                    SCAN (columns[2: L_PARTKEY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 11: L_SHIPDATE] predicate[11: L_SHIPDATE >= 1997-02-01 AND 11: L_SHIPDATE < 1997-03-01])
[end]

