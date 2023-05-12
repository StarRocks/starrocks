[sql]
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
    hive0.tpch.lineitem,
    hive0.tpch.part
where
        p_partkey = l_partkey
  and p_brand = 'Brand#35'
  and p_container = 'JUMBO CASE'
  and l_quantity < (
    select
            0.2 * avg(l_quantity)
    from
        hive0.tpch.lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{45: sum=sum(45: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{45: sum=sum(6: l_extendedprice)}] group by [[]] having [null]
            PREDICATE cast(5: l_quantity as decimal128(38, 9)) < multiply(0.2, 141: avg)
                ANALYTIC ({141: avg=avg(5: l_quantity)} [17: p_partkey] [] )
                    TOP-N (order by [[17: p_partkey ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[17]
                            SCAN (mv[lineitem_mv] columns[91: l_extendedprice, 93: l_partkey, 94: l_quantity, 107: p_brand, 108: p_container] predicate[107: p_brand = Brand#35 AND 108: p_container = JUMBO CASE])
[end]

