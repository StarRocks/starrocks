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
AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[]] having [null]
<<<<<<< HEAD
            SCAN (mv[lineitem_mv] columns[99: l_quantity, 103: l_shipinstruct, 104: l_shipmode, 112: p_brand, 113: p_container, 115: p_size, 120: l_saleprice] predicate[99: l_quantity >= 5 AND 99: l_quantity <= 35 AND 103: l_shipinstruct = DELIVER IN PERSON AND 115: p_size <= 15 AND 115: p_size >= 1 AND 112: p_brand = Brand#45 AND 113: p_container IN (SM CASE, SM BOX, SM PACK, SM PKG) AND 99: l_quantity >= 5 AND 99: l_quantity <= 15 AND 115: p_size <= 5 OR 112: p_brand = Brand#11 AND 113: p_container IN (MED BAG, MED BOX, MED PKG, MED PACK) AND 99: l_quantity >= 15 AND 99: l_quantity <= 25 AND 115: p_size <= 10 OR 112: p_brand = Brand#21 AND 113: p_container IN (LG CASE, LG BOX, LG PACK, LG PKG) AND 99: l_quantity >= 25 AND 99: l_quantity <= 35 AND 115: p_size <= 15 AND 104: l_shipmode IN (AIR, AIR REG) AND 112: p_brand IN (Brand#45, Brand#11, Brand#21) AND 113: p_container IN (SM CASE, SM BOX, SM PACK, SM PKG, MED BAG, MED BOX, MED PKG, MED PACK, LG CASE, LG BOX, LG PACK, LG PKG)])
=======
            SCAN (mv[lineitem_mv] columns[59: l_quantity, 63: l_shipinstruct, 64: l_shipmode, 72: p_brand, 73: p_container, 75: p_size, 80: l_saleprice] predicate[63: l_shipinstruct = DELIVER IN PERSON AND 75: p_size >= 1 AND 64: l_shipmode IN (AIR, AIR REG) AND 72: p_brand = Brand#11 AND 75: p_size <= 10 AND 73: p_container IN (MED BAG, MED BOX, MED PACK, MED PKG) AND 59: l_quantity <= 25.00 AND 59: l_quantity >= 15.00 OR 72: p_brand = Brand#21 AND 75: p_size <= 15 AND 73: p_container IN (LG BOX, LG CASE, LG PACK, LG PKG) AND 59: l_quantity <= 35.00 AND 59: l_quantity >= 25.00 OR 72: p_brand = Brand#45 AND 75: p_size <= 5 AND 73: p_container IN (SM BOX, SM CASE, SM PACK, SM PKG) AND 59: l_quantity <= 15.00 AND 59: l_quantity >= 5.00])
>>>>>>> 2.5.18
[end]

