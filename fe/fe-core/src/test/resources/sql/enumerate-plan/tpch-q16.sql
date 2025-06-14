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