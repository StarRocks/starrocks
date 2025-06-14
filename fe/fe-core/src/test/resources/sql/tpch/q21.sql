[result]
TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
    TOP-N (order by [[77: count DESC NULLS LAST, 2: S_NAME ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{77: count=count(77: count)}] group by [[2: S_NAME]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{77: count=count()}] group by [[2: S_NAME]] having [null]
                    INNER JOIN (join-predicate [4: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 4: S_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                LEFT ANTI JOIN (join-predicate [9: L_ORDERKEY = 59: L_ORDERKEY AND 61: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                                    LEFT SEMI JOIN (join-predicate [9: L_ORDERKEY = 41: L_ORDERKEY AND 43: L_SUPPKEY != 11: L_SUPPKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [9: L_ORDERKEY = 26: O_ORDERKEY] post-join-predicate [null])
                                            SCAN (columns[20: L_COMMITDATE, 21: L_RECEIPTDATE, 9: L_ORDERKEY, 11: L_SUPPKEY] predicate[21: L_RECEIPTDATE > 20: L_COMMITDATE])
                                            EXCHANGE SHUFFLE[26]
                                                SCAN (columns[26: O_ORDERKEY, 28: O_ORDERSTATUS] predicate[28: O_ORDERSTATUS = F])
                                        SCAN (columns[41: L_ORDERKEY, 43: L_SUPPKEY] predicate[null])
                                    SCAN (columns[70: L_COMMITDATE, 71: L_RECEIPTDATE, 59: L_ORDERKEY, 61: L_SUPPKEY] predicate[71: L_RECEIPTDATE > 70: L_COMMITDATE])
                        EXCHANGE BROADCAST
                            SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = CANADA])
[end]

