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
            and l_shipinstruct = 'DELIVER IN PERSON'
        ) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{29: sum=sum(29: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{29: sum=sum(multiply(6: L_EXTENDEDPRICE, subtract(1.0, 7: L_DISCOUNT)))}] group by [[]] having [null]
            INNER JOIN (join-predicate [2: L_PARTKEY = 18: P_PARTKEY] post-join-predicate [null])
                SCAN (columns[2: L_PARTKEY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 14: L_SHIPINSTRUCT] predicate[14: L_SHIPINSTRUCT = DELIVER IN PERSON])
                EXCHANGE BROADCAST
                    SCAN (columns[18: P_PARTKEY] predicate[null])
[end]

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
            and p_partkey = l_partkey
            and l_shipinstruct = 'DELIVER IN PERSON'
        )
   or
    (
            p_brand = 'Brand#11'
            and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        ) ;
[result]
AGGREGATE ([GLOBAL] aggregate [{29: sum=sum(29: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{29: sum=sum(multiply(6: L_EXTENDEDPRICE, subtract(1.0, 7: L_DISCOUNT)))}] group by [[]] having [null]
            INNER JOIN (join-predicate [18: P_PARTKEY = 2: L_PARTKEY AND 21: P_BRAND = Brand#45 AND 24: P_CONTAINER IN (SM CASE, SM BOX, SM PACK, SM PKG) AND 5: L_QUANTITY >= 5.0 AND 5: L_QUANTITY <= 15.0 AND 23: P_SIZE >= 1 AND 23: P_SIZE <= 5 AND 15: L_SHIPMODE IN (AIR, AIR REG) AND 14: L_SHIPINSTRUCT = DELIVER IN PERSON OR 21: P_BRAND = Brand#11 AND 24: P_CONTAINER IN (MED BAG, MED BOX, MED PKG, MED PACK)] post-join-predicate [null])
                SCAN (columns[2: L_PARTKEY, 5: L_QUANTITY, 6: L_EXTENDEDPRICE, 7: L_DISCOUNT, 14: L_SHIPINSTRUCT, 15: L_SHIPMODE] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[18: P_PARTKEY, 21: P_BRAND, 23: P_SIZE, 24: P_CONTAINER] predicate[21: P_BRAND IN (Brand#45, Brand#11) AND 24: P_CONTAINER IN (SM CASE, SM BOX, SM PACK, SM PKG, MED BAG, MED BOX, MED PKG, MED PACK)])
[end]


/**
 * ScalarRangePredicateExtractor
*/
[sql]
select v1 from t0 inner join t1 on v3 = v4 where v2 = 2 or (v2 = 3 and v3 = 3) or (v2 = 4 and v3=4)
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2 OR 2: v2 = 3 AND 3: v3 = 3 OR 2: v2 = 4 AND 3: v3 = 4 AND 2: v2 IN (2, 3, 4)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where (v2 = 2 or v2 = 3);
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 IN (2, 3)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where (v2 = 2 or v2 = 3) and (v3 = 3 or v4 = 4);
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4 AND 3: v3 = 3 OR 4: v4 = 4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 IN (2, 3)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where  v2 = 2 or (v2 = 3 and v3 = 3) or (v2 = 4 and v3=4 and v3 = 5);
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2 OR 2: v2 = 3 AND 3: v3 = 3 OR 2: v2 = 4 AND 3: v3 = 4 AND 3: v3 = 5 AND 2: v2 IN (2, 3, 4)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where  v2 = 2 or (v2 = 3 and v3 = 3) or (v2 = 4 and v3=4) or v2 = 5;
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2 OR 2: v2 = 3 AND 3: v3 = 3 OR 2: v2 = 4 AND 3: v3 = 4 OR 2: v2 = 5 AND 2: v2 IN (2, 3, 4, 5)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where  v2 in (1,2) or (v2 = 3 and v3 = 3) or (v2 in (4,5) and v3=4) and v2 = 6;
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 IN (1, 2) OR 2: v2 = 3 AND 3: v3 = 3 OR 2: v2 IN (4, 5) AND 3: v3 = 4 AND 2: v2 = 6 AND 2: v2 IN (1, 2, 3)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where  v2 = 2 or (v2 = 3 and v3 = 3) or (v2 = 4 and v3=4) or (v2 in (6,7,8) and v3 = 6);
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2 OR 2: v2 = 3 AND 3: v3 = 3 OR 2: v2 = 4 AND 3: v3 = 4 OR 2: v2 IN (6, 7, 8) AND 3: v3 = 6 AND 2: v2 IN (2, 3, 4, 6, 7, 8)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where  v2 = 2 or (v2 = 3 and v3 = 3) or (v3=4) or (v2 in (6,7,8) and v3 = 6);
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[2: v2 = 2 OR 2: v2 = 3 AND 3: v3 = 3 OR 3: v3 = 4 OR 2: v2 IN (6, 7, 8) AND 3: v3 = 6])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where (v1 = 1 AND v2 = 2) OR (v1 = 3 AND v2 = 4 AND v1 != 5)
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 1 AND 2: v2 = 2 OR 1: v1 = 3 AND 2: v2 = 4 AND 1: v1 != 5 AND 1: v1 IN (1, 3) AND 2: v2 IN (2, 4)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where (v1 = 1 AND v2 = 2) OR (v2 = 4 AND v1 = v3)
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 1 AND 2: v2 = 2 OR 2: v2 = 4 AND 1: v1 = 3: v3 AND 2: v2 IN (2, 4)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 inner join t1 on v3 = v4 where (v1 = 1 AND v2 = 2) OR (v2 = 4 AND v1 not in (1,2))
[result]
INNER JOIN (join-predicate [3: v3 = 4: v4] post-join-predicate [null])
    SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 1 AND 2: v2 = 2 OR 2: v2 = 4 AND 1: v1 NOT IN (1, 2) AND 2: v2 IN (2, 4)])
    EXCHANGE BROADCAST
        SCAN (columns[4: v4] predicate[4: v4 IS NOT NULL])
[end]

[sql]
select v1 from t0 where (v1 = 1 or v1 = 2) and v2 = v3
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IN (1, 2) AND 2: v2 = 3: v3])
[end]

[sql]
select v1 from t0 where v1 = 1 and v2 = 2 or v1 =3;
[result]
SCAN (columns[1: v1, 2: v2] predicate[1: v1 = 1 AND 2: v2 = 2 OR 1: v1 = 3 AND 1: v1 IN (1, 3)])
[end]

[sql]
select v1 from t0 where (v1 = 1 or v1 = 2) and v2 = v1
[result]
SCAN (columns[1: v1, 2: v2] predicate[1: v1 IN (1, 2) AND 2: v2 = 1: v1])
[end]

[sql]
select * from test.t0 where (v1 = 1 or v1 = 2) and (v2 = 3 or v2 = 4);
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IN (1, 2) AND 2: v2 IN (3, 4)])
[end]

[sql]
select * from test.t0 where (v1 = 1 and v2 = 2) or (v1 = 3 and v2 = 4);
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 = 1 AND 2: v2 = 2 OR 1: v1 = 3 AND 2: v2 = 4 AND 1: v1 IN (1, 3) AND 2: v2 IN (2, 4)])
[end]

[sql]
select * from t0 where (v1 = 1 or v1 = 2 ) and v1 in (1,2)
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 IN (1, 2)])
[end]

[sql]
select * from t0 where v1 > 10 or v1 = 1 or v1 = 2
[result]
SCAN (columns[1: v1, 2: v2, 3: v3] predicate[1: v1 > 10 OR 1: v1 = 1 OR 1: v1 = 2 AND 1: v1 >= 1])
[end]