[sql]
select
    p_brand,
    p_type,
    p_size,
    ps_suppkey as supplier_cnt
from
    partsupp,
    part
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#43'
  and p_type not like 'PROMO BURNISHED%'
  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)
[result]
INNER JOIN (join-predicate [6: p_partkey = 1: ps_partkey] post-join-predicate [null])
    SCAN (table[part] columns[6: p_partkey, 9: p_brand, 10: p_type, 11: p_size] predicate[9: p_brand != Brand#43 AND NOT 10: p_type LIKE PROMO BURNISHED% AND 11: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)])
    EXCHANGE SHUFFLE[1]
        SCAN (table[partsupp] columns[1: ps_partkey, 2: ps_suppkey] predicate[null])
[end]

