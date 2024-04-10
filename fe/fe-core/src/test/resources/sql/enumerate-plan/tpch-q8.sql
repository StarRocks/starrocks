[planCount]
10
[plan-1]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [37: O_CUSTKEY = 46: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [19: L_ORDERKEY = 36: O_ORDERKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY AND 11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                                            SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                EXCHANGE BROADCAST
                                    SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
[end]
[plan-2]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [37: O_CUSTKEY = 46: C_CUSTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [19: L_ORDERKEY = 36: O_ORDERKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY AND 11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                                            SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[20]
                                            EXCHANGE SHUFFLE[20, 21]
                                                SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                EXCHANGE BROADCAST
                                    SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
[end]
[plan-3]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                EXCHANGE BROADCAST
                                                    INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-4]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                EXCHANGE SHUFFLE[19]
                                                    INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-5]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                        EXCHANGE SHUFFLE[37]
                                            EXCHANGE SHUFFLE[37]
                                                INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                    SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                    EXCHANGE BROADCAST
                                                        INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                            SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-6]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                    INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                        SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                        EXCHANGE SHUFFLE[37]
                                            EXCHANGE SHUFFLE[37]
                                                INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                    SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                    EXCHANGE SHUFFLE[19]
                                                        INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                            SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-7]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[21]
                                EXCHANGE SHUFFLE[21]
                                    INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                            SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                    SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                    EXCHANGE BROADCAST
                                                        INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                            SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-8]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[21]
                                EXCHANGE SHUFFLE[21]
                                    INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                            SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                    SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                    EXCHANGE SHUFFLE[19]
                                                        INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                            SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-9]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[21]
                                EXCHANGE SHUFFLE[21]
                                    INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                            SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                            EXCHANGE SHUFFLE[37]
                                                EXCHANGE SHUFFLE[37]
                                                    INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                        SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                        EXCHANGE BROADCAST
                                                            INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                                SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                                EXCHANGE BROADCAST
                                                                    SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]
[plan-10]
TOP-N (order by [[69: year ASC NULLS FIRST]])
    TOP-N (order by [[69: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{72: sum=sum(72: sum), 73: sum=sum(73: sum)}] group by [[69: year]] having [null]
            EXCHANGE SHUFFLE[69]
                AGGREGATE ([LOCAL] aggregate [{72: sum=sum(71: case), 73: sum=sum(70: expr)}] group by [[69: year]] having [null]
                    INNER JOIN (join-predicate [14: S_NATIONKEY = 60: N_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                            EXCHANGE SHUFFLE[21]
                                EXCHANGE SHUFFLE[21]
                                    INNER JOIN (join-predicate [49: C_NATIONKEY = 55: N_NATIONKEY] post-join-predicate [null])
                                        INNER JOIN (join-predicate [46: C_CUSTKEY = 37: O_CUSTKEY] post-join-predicate [null])
                                            SCAN (columns[49: C_NATIONKEY, 46: C_CUSTKEY] predicate[null])
                                            EXCHANGE SHUFFLE[37]
                                                EXCHANGE SHUFFLE[37]
                                                    INNER JOIN (join-predicate [36: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                                                        SCAN (columns[36: O_ORDERKEY, 37: O_CUSTKEY, 40: O_ORDERDATE] predicate[40: O_ORDERDATE >= 1995-01-01 AND 40: O_ORDERDATE <= 1996-12-31])
                                                        EXCHANGE SHUFFLE[19]
                                                            INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                                                SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                                                EXCHANGE BROADCAST
                                                                    SCAN (columns[1: P_PARTKEY, 5: P_TYPE] predicate[5: P_TYPE = ECONOMY ANODIZED STEEL])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [57: N_REGIONKEY = 65: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[55: N_NATIONKEY, 57: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[65: R_REGIONKEY, 66: R_NAME] predicate[66: R_NAME = MIDDLE EAST])
                        EXCHANGE BROADCAST
                            SCAN (columns[60: N_NATIONKEY, 61: N_NAME] predicate[null])
[end]