[sql]
select
    count(1)
from
    partsupp,
    part
where
        p_partkey = ps_partkey
  and p_brand <> 'Brand#43'
  and p_type not like 'PROMO BURNISHED%'
  and p_size in (31, 43, 9, 6, 18, 11, 25, 1)
  and ps_suppkey not in (
    select
        s_suppkey
    from
        supplier
    where
            s_comment like '%Customer%Complaints%'
)
group by
    p_brand,
    p_type,
    p_size;

[result]
AGGREGATE ([GLOBAL] aggregate [{23: count=count(23: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{23: count=count(1)}] group by [[]] having [null]
            NULL AWARE LEFT ANTI JOIN (join-predicate [2: ps_suppkey = 15: s_suppkey] post-join-predicate [null])
                INNER JOIN (join-predicate [1: ps_partkey = 6: p_partkey] post-join-predicate [null])
                    SCAN (table[partsupp] columns[1: ps_partkey, 2: ps_suppkey] predicate[null])
                    EXCHANGE SHUFFLE[6]
                        SCAN (table[part] columns[6: p_partkey, 9: p_brand, 10: p_type, 11: p_size] predicate[9: p_brand != Brand#43 AND NOT 10: p_type LIKE PROMO BURNISHED% AND 11: p_size IN (31, 43, 9, 6, 18, 11, 25, 1)])
                EXCHANGE BROADCAST
                    SCAN (table[supplier] columns[21: s_comment, 15: s_suppkey] predicate[21: s_comment LIKE %Customer%Complaints%])
[end]

