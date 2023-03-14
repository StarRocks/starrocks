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
--             0.2 * avg(l_quantity)
        0.2 * sum(l_quantity) / count(l_quantity)
    from
        lineitem
    where
            l_partkey = p_partkey
) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{46: sum=sum(7: l_extendedprice)}] group by [[]] having [null]
            PREDICATE cast(6: l_quantity as decimal128(38, 9)) < divide(multiply(0.2, 150: sum), cast(151: count as decimal128(38, 0)))
                ANALYTIC ({150: sum=sum(6: l_quantity), 151: count=count(6: l_quantity)} [17: p_partkey] [] )
                    TOP-N (order by [[17: p_partkey ASC NULLS FIRST]])
                        EXCHANGE SHUFFLE[17]
                            SCAN (mv[lineitem_mv] columns[99: l_extendedprice, 101: l_partkey, 102: l_quantity, 115: p_brand, 116: p_container] predicate[115: p_brand = Brand#35 AND 116: p_container = JUMBO CASE])
[end]

