[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 315
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate limit 100;
[planCount]
16
[plan-1]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-2]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-3]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                            SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-4]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                EXCHANGE BROADCAST
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                            SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-5]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-6]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-7]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                    AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-8]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                    AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-9]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]
[plan-10]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                    AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                        AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                            SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]
[plan-11]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE SHUFFLE[37]
                                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]
[plan-12]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                    EXCHANGE SHUFFLE[10]
                        LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE SHUFFLE[37]
                                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                    AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]
[plan-13]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                EXCHANGE SHUFFLE[10]
                    LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                        EXCHANGE SHUFFLE[37]
                            AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-14]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                EXCHANGE SHUFFLE[10]
                    LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                        EXCHANGE SHUFFLE[37]
                            AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]
[plan-15]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                EXCHANGE SHUFFLE[10]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE SHUFFLE[37]
                                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]
[plan-16]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                EXCHANGE SHUFFLE[10]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE SHUFFLE[37]
                                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(54: sum)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                    AGGREGATE ([LOCAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                                        SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
[end]