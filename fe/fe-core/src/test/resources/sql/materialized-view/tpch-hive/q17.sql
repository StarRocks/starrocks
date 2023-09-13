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
            PREDICATE cast(5: l_quantity as decimal128(38, 9)) < multiply(0.2, 145: avg)
                ANALYTIC ({145: avg=avg(5: l_quantity)} [17: p_partkey] [] )
                    TOP-N (order by [[17: p_partkey ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[17]
                            SCAN (mv[lineitem_mv] columns[76: l_extendedprice, 78: l_partkey, 79: l_quantity, 92: p_brand, 93: p_container] predicate[92: p_brand = Brand#35 AND 93: p_container = JUMBO CASE])
[end]

