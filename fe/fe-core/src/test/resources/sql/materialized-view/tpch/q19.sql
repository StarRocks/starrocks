[result]
AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[60: l_quantity, 64: l_shipinstruct, 65: l_shipmode, 73: p_brand, 74: p_container, 76: p_size, 81: l_saleprice] predicate[64: l_shipinstruct = DELIVER IN PERSON AND 76: p_size >= 1 AND 65: l_shipmode IN (AIR, AIR REG) AND 73: p_brand = Brand#11 AND 76: p_size <= 10 AND 74: p_container IN (MED BAG, MED BOX, MED PACK, MED PKG) AND 60: l_quantity >= 15.00 AND 60: l_quantity <= 25.00 OR 73: p_brand = Brand#21 AND 76: p_size <= 15 AND 74: p_container IN (LG BOX, LG CASE, LG PACK, LG PKG) AND 60: l_quantity >= 25.00 AND 60: l_quantity <= 35.00 OR 73: p_brand = Brand#45 AND 76: p_size <= 5 AND 74: p_container IN (SM BOX, SM CASE, SM PACK, SM PKG) AND 60: l_quantity >= 5.00 AND 60: l_quantity <= 15.00])
[end]

