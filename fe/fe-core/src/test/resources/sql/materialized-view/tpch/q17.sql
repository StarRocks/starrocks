[result]
AGGREGATE ([GLOBAL] aggregate [{45: sum=sum(45: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{45: sum=sum(7: l_extendedprice)}] group by [[]] having [null]
            PREDICATE cast(6: l_quantity as DECIMAL128(38,9)) < multiply(0.2, 144: avg)
                ANALYTIC ({144: avg=avg(6: l_quantity)} [17: p_partkey] [] )
                    TOP-N (order by [[17: p_partkey ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[17]
                            SCAN (mv[lineitem_mv] columns[106: l_extendedprice, 108: l_partkey, 109: l_quantity, 122: p_brand, 123: p_container] predicate[122: p_brand = Brand#35 AND 123: p_container = JUMBO CASE])
[end]

