
[result]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                FETCH (columns[[1: PS_PARTKEY,3: PS_AVAILQTY,4: PS_SUPPLYCOST]])
                    INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                        SCAN (columns[2: PS_SUPPKEY, 45: ROW_ID] predicate[null])
                        EXCHANGE BROADCAST
                            FETCH (columns[[7: S_SUPPKEY]])
                                INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[10: S_NATIONKEY, 46: ROW_ID] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
                                LookUp
                    LookUp
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(42: sum)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{42: sum=sum(multiply(25: PS_SUPPLYCOST, cast(24: PS_AVAILQTY as double)))}] group by [[]] having [null]
                                FETCH (columns[[24: PS_AVAILQTY,25: PS_SUPPLYCOST]])
                                    INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[23: PS_SUPPKEY, 47: ROW_ID] predicate[null])
                                        EXCHANGE BROADCAST
                                            FETCH (columns[[28: S_SUPPKEY]])
                                                INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                                    SCAN (columns[31: S_NATIONKEY, 48: ROW_ID] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
                                                LookUp
                                    LookUp
[end]

