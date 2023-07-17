[sql]
select
    sum(l_extendedprice* (1 - l_discount)) as revenue
from
    hive0.tpch.lineitem,
    hive0.tpch.part
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
            SCAN (mv[lineitem_mv] columns[60: l_quantity, 64: l_shipinstruct, 65: l_shipmode, 73: p_brand, 74: p_container, 76: p_size, 81: l_saleprice] predicate[64: l_shipinstruct = DELIVER IN PERSON AND 76: p_size >= 1 AND 73: p_brand = Brand#45 AND 74: p_container IN (SM CASE, SM BOX, SM PACK, SM PKG) AND 60: l_quantity >= 5 AND 60: l_quantity <= 15 AND 76: p_size <= 5 OR 73: p_brand = Brand#11 AND 74: p_container IN (MED BAG, MED BOX, MED PKG, MED PACK) AND 60: l_quantity >= 15 AND 60: l_quantity <= 25 AND 76: p_size <= 10 OR 73: p_brand = Brand#21 AND 74: p_container IN (LG CASE, LG BOX, LG PACK, LG PKG) AND 60: l_quantity >= 25 AND 60: l_quantity <= 35 AND 76: p_size <= 15 AND 65: l_shipmode IN (AIR, AIR REG)])
=======
            SCAN (mv[lineitem_mv] columns[75: l_quantity, 79: l_shipinstruct, 80: l_shipmode, 88: p_brand, 89: p_container, 91: p_size, 96: l_saleprice] predicate[79: l_shipinstruct = DELIVER IN PERSON AND 91: p_size >= 1 AND 88: p_brand = Brand#45 AND 89: p_container IN (SM CASE, SM BOX, SM PACK, SM PKG) AND 75: l_quantity >= 5.0 AND 75: l_quantity <= 15.0 AND 91: p_size <= 5 OR 88: p_brand = Brand#11 AND 89: p_container IN (MED BAG, MED BOX, MED PKG, MED PACK) AND 75: l_quantity >= 15.0 AND 75: l_quantity <= 25.0 AND 91: p_size <= 10 OR 88: p_brand = Brand#21 AND 89: p_container IN (LG CASE, LG BOX, LG PACK, LG PKG) AND 75: l_quantity >= 25.0 AND 75: l_quantity <= 35.0 AND 91: p_size <= 15 AND 80: l_shipmode IN (AIR, AIR REG)])
>>>>>>> 9408b7a6e9 ([BugFix] Decimal cast to string on FE (#27235))
[end]

