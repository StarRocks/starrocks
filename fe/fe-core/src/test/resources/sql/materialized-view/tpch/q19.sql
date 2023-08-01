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
            SCAN (mv[lineitem_mv] columns[95: l_quantity, 99: l_shipinstruct, 100: l_shipmode, 108: p_brand, 109: p_container, 111: p_size, 116: l_saleprice] predicate[99: l_shipinstruct = DELIVER IN PERSON AND 100: l_shipmode = AIR OR 100: l_shipmode = AIR REG AND 108: p_brand = Brand#11 AND 111: p_size <= 10 AND 109: p_container = MED BAG OR 109: p_container = MED BOX OR 109: p_container = MED PACK OR 109: p_container = MED PKG AND 95: l_quantity <= 25.00 AND 95: l_quantity >= 15.00 OR 108: p_brand = Brand#21 AND 111: p_size <= 15 AND 109: p_container = LG BOX OR 109: p_container = LG CASE OR 109: p_container = LG PACK OR 109: p_container = LG PKG AND 95: l_quantity <= 35.00 AND 95: l_quantity >= 25.00 OR 108: p_brand = Brand#45 AND 111: p_size <= 5 AND 109: p_container = SM BOX OR 109: p_container = SM CASE OR 109: p_container = SM PACK OR 109: p_container = SM PKG AND 95: l_quantity <= 15.00 AND 95: l_quantity >= 5.00 AND 111: p_size >= 1])
[end]

