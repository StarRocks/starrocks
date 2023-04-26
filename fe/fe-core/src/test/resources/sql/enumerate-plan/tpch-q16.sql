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
[planCount]
2
[plan-1]
TOP-N (order by [[26: count DESC NULLS LAST, 10: P_BRAND ASC NULLS FIRST, 11: P_TYPE ASC NULLS FIRST, 12: P_SIZE ASC NULLS FIRST]])
    TOP-N (order by [[26: count DESC NULLS LAST, 10: P_BRAND ASC NULLS FIRST, 11: P_TYPE ASC NULLS FIRST, 12: P_SIZE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{26: count=count(2: PS_SUPPKEY)}] group by [[10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
            AGGREGATE ([DISTINCT_GLOBAL] aggregate [{}] group by [[2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
                EXCHANGE SHUFFLE[10, 11, 12]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
                        NULL AWARE LEFT ANTI JOIN (join-predicate [2: PS_SUPPKEY = 17: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: PS_PARTKEY = 7: P_PARTKEY] post-join-predicate [null])
                                SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[7: P_PARTKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE] predicate[10: P_BRAND != Brand#43 AND NOT 11: P_TYPE LIKE PROMO BURNISHED% AND 12: P_SIZE IN (31, 43, 9, 6, 18, 11, 25, 1)])
                            EXCHANGE BROADCAST
                                SCAN (columns[17: S_SUPPKEY, 23: S_COMMENT] predicate[23: S_COMMENT LIKE %Customer%Complaints%])
[end]
[plan-2]
TOP-N (order by [[26: count DESC NULLS LAST, 10: P_BRAND ASC NULLS FIRST, 11: P_TYPE ASC NULLS FIRST, 12: P_SIZE ASC NULLS FIRST]])
    TOP-N (order by [[26: count DESC NULLS LAST, 10: P_BRAND ASC NULLS FIRST, 11: P_TYPE ASC NULLS FIRST, 12: P_SIZE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{26: count=count(2: PS_SUPPKEY)}] group by [[10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
            AGGREGATE ([DISTINCT_GLOBAL] aggregate [{}] group by [[2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
                EXCHANGE SHUFFLE[10, 11, 12]
                    AGGREGATE ([LOCAL] aggregate [{}] group by [[2: PS_SUPPKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE]] having [null]
                        NULL AWARE LEFT ANTI JOIN (join-predicate [2: PS_SUPPKEY = 17: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [1: PS_PARTKEY = 7: P_PARTKEY] post-join-predicate [null])
                                SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY] predicate[null])
                                EXCHANGE SHUFFLE[7]
                                    SCAN (columns[7: P_PARTKEY, 10: P_BRAND, 11: P_TYPE, 12: P_SIZE] predicate[10: P_BRAND != Brand#43 AND NOT 11: P_TYPE LIKE PROMO BURNISHED% AND 12: P_SIZE IN (31, 43, 9, 6, 18, 11, 25, 1)])
                            EXCHANGE BROADCAST
                                SCAN (columns[17: S_SUPPKEY, 23: S_COMMENT] predicate[23: S_COMMENT LIKE %Customer%Complaints%])
[end]