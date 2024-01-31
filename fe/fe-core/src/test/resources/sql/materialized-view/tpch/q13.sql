[result]
TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
    TOP-N (order by [[19: count DESC NULLS LAST, 18: count DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{19: count=count(19: count)}] group by [[18: count]] having [null]
            EXCHANGE SHUFFLE[18]
                AGGREGATE ([LOCAL] aggregate [{19: count=count()}] group by [[18: count]] having [null]
                    AGGREGATE ([GLOBAL] aggregate [{18: count=count(9: o_orderkey)}] group by [[1: c_custkey]] having [null]
                        LEFT OUTER JOIN (join-predicate [: c_custkey = : o_custkey] post-join-predicate [null])
                            SCAN (table[customer] columns[: c_custkey] predicate[null])
                            EXCHANGE SHUFFLE[]
                                SCAN (table[orders] columns[: o_comment, : o_orderkey, : o_custkey] predicate[NOT : o_comment LIKE %unusual%deposits%])
[end]

