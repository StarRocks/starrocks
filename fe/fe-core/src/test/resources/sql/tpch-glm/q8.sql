
[result]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    FETCH (columns[[61: N_NAME]])
                        INNER JOIN (join-predicate [60: N_NATIONKEY = 14: S_NATIONKEY] post-join-predicate [null])
                            SCAN (columns[60: N_NATIONKEY, 77: ROW_ID] predicate[null])
                            EXCHANGE SHUFFLE[14]
                                FETCH (columns[[14: S_NATIONKEY]])
                                    INNER JOIN (join-predicate [21: L_SUPPKEY = 11: S_SUPPKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [19: L_ORDERKEY = 36: O_ORDERKEY] post-join-predicate [null])
                                            FETCH (columns[[19: L_ORDERKEY,21: L_SUPPKEY,24: L_EXTENDEDPRICE,25: L_DISCOUNT]])
                                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                    SCAN (columns[20: L_PARTKEY, 78: ROW_ID] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                                LookUp
                                            EXCHANGE SHUFFLE[36]
                                                FETCH (columns[[36: O_ORDERKEY]])
                                                    INNER JOIN (join-predicate [37: O_CUSTKEY = 46: C_CUSTKEY] post-join-predicate [null])
                                                        SCAN (columns[37: O_CUSTKEY, 40: O_ORDERDATE, 79: ROW_ID] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                        EXCHANGE BROADCAST
                                                            FETCH (columns[[46: C_CUSTKEY]])
                                                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                                                    SCAN (columns[49: C_NATIONKEY, 80: ROW_ID] predicate[null])
                                                                    EXCHANGE BROADCAST
                                                                        FETCH (columns[[55: N_NATIONKEY]])
                                                                            INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                                                SCAN (columns[81: ROW_ID, 57: N_REGIONKEY] predicate[null])
                                                                                EXCHANGE BROADCAST
                                                                                    SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                                                                            LookUp
                                                                LookUp
                                                    LookUp
                                        EXCHANGE BROADCAST
                                            SCAN (columns[82: ROW_ID, 11: S_SUPPKEY] predicate[null])
                                    LookUp
                        LookUp
[end]

