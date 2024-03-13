[planCount]
16
[plan-1]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-2]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-3]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-4]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-5]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-6]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-7]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                        SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-8]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                        SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-9]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-10]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-11]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-12]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                            EXCHANGE SHUFFLE[30]
                                                AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-13]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-14]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-15]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                        SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]
[plan-16]
TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
    TOP-N (order by [[1: S_SUPPKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: S_SUPPKEY = 11: L_SUPPKEY] post-join-predicate [null])
            SCAN (columns[1: S_SUPPKEY, 2: S_NAME, 3: S_ADDRESS, 5: S_PHONE] predicate[null])
            EXCHANGE SHUFFLE[11]
                INNER JOIN (join-predicate [27: sum = 47: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{27: sum=sum(27: sum)}] group by [[11: L_SUPPKEY]] having [27: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[11]
                            AGGREGATE ([LOCAL] aggregate [{27: sum=sum(26: expr)}] group by [[11: L_SUPPKEY]] having [null]
                                SCAN (columns[19: L_SHIPDATE, 11: L_SUPPKEY, 14: L_EXTENDEDPRICE, 15: L_DISCOUNT] predicate[19: L_SHIPDATE >= 1995-07-01 AND 19: L_SHIPDATE < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 47: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{47: max=max(47: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{47: max=max(46: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{46: sum=sum(46: sum)}] group by [[30: L_SUPPKEY]] having [null]
                                                EXCHANGE SHUFFLE[30]
                                                    AGGREGATE ([LOCAL] aggregate [{46: sum=sum(45: expr)}] group by [[30: L_SUPPKEY]] having [null]
                                                        SCAN (columns[33: L_EXTENDEDPRICE, 34: L_DISCOUNT, 38: L_SHIPDATE, 30: L_SUPPKEY] predicate[38: L_SHIPDATE >= 1995-07-01 AND 38: L_SHIPDATE < 1995-10-01])
[end]