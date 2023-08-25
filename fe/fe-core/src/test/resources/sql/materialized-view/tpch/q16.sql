[sql]
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
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
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size ;
[result]
TOP-N (order by [[23: count DESC NULLS LAST, 9: p_brand ASC NULLS FIRST, 10: p_type ASC NULLS FIRST, 11: p_size ASC NULLS FIRST]])
    TOP-N (order by [[23: count DESC NULLS LAST, 9: p_brand ASC NULLS FIRST, 10: p_type ASC NULLS FIRST, 11: p_size ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{23: count=count(2: ps_suppkey)}] group by [[9: p_brand, 10: p_type, 11: p_size]] having [null]
            AGGREGATE ([DISTINCT_GLOBAL] aggregate [{}] group by [[2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size]] having [null]
                EXCHANGE SHUFFLE[9, 10, 11]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[2: ps_suppkey, 9: p_brand, 10: p_type, 11: p_size]] having [null]
                        NULL AWARE LEFT ANTI JOIN (join-predicate [2: ps_suppkey = 15: s_suppkey] post-join-predicate [null])
                            SCAN (mv[partsupp_mv] columns[109: p_size, 110: p_type, 111: p_brand, 113: ps_suppkey] predicate[109: p_size = 1 OR 109: p_size = 11 OR 109: p_size = 18 OR 109: p_size = 25 OR 109: p_size = 31 OR 109: p_size = 43 OR 109: p_size = 6 OR 109: p_size = 9 AND 111: p_brand < Brand#43 OR 111: p_brand > Brand#43 AND NOT 110: p_type LIKE PROMO BURNISHED%])
                            EXCHANGE BROADCAST
                                SCAN (table[supplier] columns[21: s_comment, 15: s_suppkey] predicate[21: s_comment LIKE %Customer%Complaints%])
end]

