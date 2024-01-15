[planCount]
16
[plan-1]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                        SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-2]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                    SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                            SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-3]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(21: sum)}] group by [[1: PS_PARTKEY]] having [null]
                AGGREGATE ([LOCAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                    INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-4]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(21: sum)}] group by [[1: PS_PARTKEY]] having [null]
                AGGREGATE ([LOCAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                    INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                        SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-5]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                        SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-6]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                    SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                            SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-7]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(21: sum)}] group by [[1: PS_PARTKEY]] having [null]
                AGGREGATE ([LOCAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                    INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]
[plan-8]
TOP-N (order by [[21: sum DESC NULLS LAST]])
    TOP-N (order by [[21: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [21: sum > 43: expr] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{21: sum=sum(21: sum)}] group by [[1: PS_PARTKEY]] having [null]
                AGGREGATE ([LOCAL] aggregate [{21: sum=sum(20: expr)}] group by [[1: PS_PARTKEY]] having [null]
                    INNER JOIN (join-predicate [2: PS_SUPPKEY = 7: S_SUPPKEY] post-join-predicate [null])
                        SCAN (columns[1: PS_PARTKEY, 2: PS_SUPPKEY, 3: PS_AVAILQTY, 4: PS_SUPPLYCOST] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [10: S_NATIONKEY = 15: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[7: S_SUPPKEY, 10: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[15: N_NATIONKEY, 16: N_NAME] predicate[16: N_NAME = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{42: sum=sum(41: expr)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            INNER JOIN (join-predicate [23: PS_SUPPKEY = 28: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[23: PS_SUPPKEY, 24: PS_AVAILQTY, 25: PS_SUPPLYCOST] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [31: S_NATIONKEY = 36: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[28: S_SUPPKEY, 31: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[36: N_NATIONKEY, 37: N_NAME] predicate[37: N_NAME = PERU])
[end]