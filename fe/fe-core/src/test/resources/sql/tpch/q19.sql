[sql]
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    lineitem,
    part
where
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#45'
            and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
            and l_quantity >= 5 and l_quantity <= 5 + 10
            and p_size between 1 and 5
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#11'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
            and l_quantity >= 15 and l_quantity <= 15 + 10
            and p_size between 1 and 10
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
                p_partkey = l_partkey
            and p_brand = 'Brand#21'
            and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
            and l_quantity >= 25 and l_quantity <= 25 + 10
            and p_size between 1 and 15
            and l_shipmode in ('AIR', 'AIR REG')
            and l_shipinstruct = 'DELIVER IN PERSON'
    ) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{29: sum=sum(29: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{29: sum=sum(multiply(6: L_EXTENDEDPRICE, subtract(1, 7: L_DISCOUNT)))}] group by [[]] having [null]
            INNER JOIN (join-predicate [18: P_PARTKEY = 2: L_PARTKEY AND 21: P_BRAND = Brand#45 AND 24: P_CONTAINER IN (SM CASE, SM BOX, SM PACK, SM PKG) AND 5: L_QUANTITY >= 5 AND 5: L_QUANTITY <= 15 AND 23: P_SIZE <= 5 OR 21: P_BRAND = Brand#11 AND 24: P_CONTAINER IN (MED BAG, MED BOX, MED PKG, MED PACK) AND 5: L_QUANTITY >= 15 AND 5: L_QUANTITY <= 25 AND 23: P_SIZE <= 10 OR 21: P_BRAND = Brand#21 AND 24: P_CONTAINER IN (LG CASE, LG BOX, LG PACK, LG PKG) AND 5: L_QUANTITY >= 25 AND 5: L_QUANTITY <= 35 AND 23: P_SIZE <= 15] post-join-predicate [null])
                SCAN (columns[18: P_PARTKEY, 21: P_BRAND, 23: P_SIZE, 24: P_CONTAINER] predicate[21: P_BRAND IN (Brand#45, Brand#11, Brand#21) AND 23: P_SIZE <= 15 AND 24: P_CONTAINER IN (SM CASE, SM BOX, SM PACK, SM PKG, MED BAG, MED BOX, MED PKG, MED PACK, LG CASE, LG BOX, LG PACK, LG PKG) AND 23: P_SIZE >= 1])
                EXCHANGE SHUFFLE[2]
                    SCAN (columns[2: L_PARTKEY, 5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 14: L_SHIPINSTRUCT, 15: L_SHIPMODE] predicate[5: L_QUANTITY >= 5 AND 5: L_QUANTITY <= 35 AND 15: L_SHIPMODE IN (AIR, AIR REG) AND 14: L_SHIPINSTRUCT = DELIVER IN PERSON])
[end]

