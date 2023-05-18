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
--         0.2 * sum(l_quantity) / count(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{45: sum=sum(45: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{45: sum=sum(7: l_extendedprice)}] group by [[]] having [null]
            PREDICATE cast(6: l_quantity as decimal128(38, 9)) < multiply(0.2, 149: avg)
                ANALYTIC ({149: avg=avg(6: l_quantity)} [17: p_partkey] [] )
                    TOP-N (order by [[17: p_partkey ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[17]
                            SCAN (mv[lineitem_mv] columns[111: l_extendedprice, 113: l_partkey, 114: l_quantity, 127: p_brand, 128: p_container] predicate[127: p_brand = Brand#35 AND 128: p_container = JUMBO CASE])
[end]

