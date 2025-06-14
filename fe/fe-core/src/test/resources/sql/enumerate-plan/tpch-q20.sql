[planCount]
2
[plan-1]
TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
        RIGHT SEMI JOIN (join-predicate [15: PS_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[15]
                INNER JOIN (join-predicate [14: PS_PARTKEY = 32: L_PARTKEY AND 15: PS_SUPPKEY = 33: L_SUPPKEY AND cast(16: PS_AVAILQTY as double) > multiply(0.5, 48: sum)] post-join-predicate [null])
                    LEFT SEMI JOIN (join-predicate [14: PS_PARTKEY = 20: P_PARTKEY] post-join-predicate [null])
                        SCAN (columns[14: PS_PARTKEY, 15: PS_SUPPKEY, 16: PS_AVAILQTY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[20: P_PARTKEY, 21: P_NAME] predicate[21: P_NAME LIKE sienna%])
                    EXCHANGE SHUFFLE[32]
                        AGGREGATE ([GLOBAL] aggregate [{48: sum=sum(48: sum)}] group by [[33: L_SUPPKEY, 32: L_PARTKEY]] having [null]
                            EXCHANGE SHUFFLE[32, 33]
                                AGGREGATE ([LOCAL] aggregate [{48: sum=sum(35: L_QUANTITY)}] group by [[33: L_SUPPKEY, 32: L_PARTKEY]] having [null]
                                    SCAN (columns[33: L_SUPPKEY, 35: L_QUANTITY, 41: L_SHIPDATE, 32: L_PARTKEY] predicate[41: L_SHIPDATE >= 1993-01-01 AND 41: L_SHIPDATE < 1994-01-01])
            EXCHANGE SHUFFLE[1]
                INNER JOIN (join-predicate [4: S_NATIONKEY = 9: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 4: S_NATIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[9: N_NATIONKEY, 10: N_NAME] predicate[10: N_NAME = ARGENTINA])
[end]
[plan-2]
TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[2: S_NAME ASC NULLS FIRST]])
        RIGHT SEMI JOIN (join-predicate [15: PS_SUPPKEY = 1: S_SUPPKEY] post-join-predicate [null])
            EXCHANGE SHUFFLE[15]
                INNER JOIN (join-predicate [14: PS_PARTKEY = 32: L_PARTKEY AND 15: PS_SUPPKEY = 33: L_SUPPKEY AND cast(16: PS_AVAILQTY as double) > multiply(0.5, 48: sum)] post-join-predicate [null])
                    LEFT SEMI JOIN (join-predicate [14: PS_PARTKEY = 20: P_PARTKEY] post-join-predicate [null])
                        SCAN (columns[14: PS_PARTKEY, 15: PS_SUPPKEY, 16: PS_AVAILQTY] predicate[null])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: P_PARTKEY, 21: P_NAME] predicate[21: P_NAME LIKE sienna%])
                    EXCHANGE SHUFFLE[32]
                        AGGREGATE ([GLOBAL] aggregate [{48: sum=sum(48: sum)}] group by [[33: L_SUPPKEY, 32: L_PARTKEY]] having [null]
                            EXCHANGE SHUFFLE[32, 33]
                                AGGREGATE ([LOCAL] aggregate [{48: sum=sum(35: L_QUANTITY)}] group by [[33: L_SUPPKEY, 32: L_PARTKEY]] having [null]
                                    SCAN (columns[33: L_SUPPKEY, 35: L_QUANTITY, 41: L_SHIPDATE, 32: L_PARTKEY] predicate[41: L_SHIPDATE >= 1993-01-01 AND 41: L_SHIPDATE < 1994-01-01])
            EXCHANGE SHUFFLE[1]
                INNER JOIN (join-predicate [4: S_NATIONKEY = 9: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 4: S_NATIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[9: N_NATIONKEY, 10: N_NAME] predicate[10: N_NAME = ARGENTINA])
[end]