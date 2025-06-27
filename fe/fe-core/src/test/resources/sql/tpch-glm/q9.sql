
[result]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    FETCH (columns[[39: PS_SUPPLYCOST]])
                        INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 11: S_SUPPKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21]
                                    FETCH (columns[[46: O_ORDERDATE]])
                                        INNER JOIN (join-predicate [19: L_ORDERKEY = 42: O_ORDERKEY] post-join-predicate [null])
                                            FETCH (columns[[19: L_ORDERKEY,21: L_SUPPKEY,23: L_QUANTITY,24: L_EXTENDEDPRICE,25: L_DISCOUNT]])
                                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                    SCAN (columns[20: L_PARTKEY, 60: ROW_ID] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                                LookUp
                                            EXCHANGE SHUFFLE[42]
                                                SCAN (columns[42: O_ORDERKEY, 61: ROW_ID] predicate[null])
                                        LookUp
                                EXCHANGE SHUFFLE[11]
                                    FETCH (columns[[53: N_NAME],[11: S_SUPPKEY]])
                                        INNER JOIN (join-predicate [52: N_NATIONKEY = 14: S_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[52: N_NATIONKEY, 62: ROW_ID] predicate[null])
                                            EXCHANGE SHUFFLE[14]
                                                SCAN (columns[14: S_NATIONKEY, 63: ROW_ID] predicate[null])
                                        LookUp
                            EXCHANGE SHUFFLE[37]
                                SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 64: ROW_ID] predicate[null])
                        LookUp
[end]

