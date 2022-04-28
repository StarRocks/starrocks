[sql]
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#35'
  and p_container = 'JUMBO CASE'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{48: sum=sum(48: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{48: sum=sum(6: L_EXTENDEDPRICE)}] group by [[]] having [null]
            INNER JOIN (join-predicate [2: L_PARTKEY = 18: P_PARTKEY AND 5: L_QUANTITY < multiply(0.2, 45: avg)] post-join-predicate [null])
                SCAN (columns[2: L_PARTKEY, 5: L_QUANTITY, 6: L_EXTENDEDPRICE] predicate[null])
                EXCHANGE BROADCAST
                    INNER JOIN (join-predicate [29: L_PARTKEY = 18: P_PARTKEY] post-join-predicate [null])
                        AGGREGATE ([GLOBAL] aggregate [{45: avg=avg(45: avg)}] group by [[29: L_PARTKEY]] having [null]
                            EXCHANGE SHUFFLE[29]
                                AGGREGATE ([LOCAL] aggregate [{45: avg=avg(32: L_QUANTITY)}] group by [[29: L_PARTKEY]] having [null]
                                    SCAN (columns[29: L_PARTKEY, 32: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[18]
                            SCAN (columns[18: P_PARTKEY, 21: P_BRAND, 24: P_CONTAINER] predicate[21: P_BRAND = Brand#35 AND 24: P_CONTAINER = JUMBO CASE])
[end]

