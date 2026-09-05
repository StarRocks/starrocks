[result]
TOP-N (order by [[18: sum DESC NULLS LAST]])
    TOP-N (order by [[18: sum DESC NULLS LAST]])
        INNER JOIN (join-predicate [cast(18: sum as double) > cast(37: expr as double)] post-join-predicate [null])
            AGGREGATE ([GLOBAL] aggregate [{18: sum=sum(18: sum)}] group by [[1: ps_partkey]] having [null]
                EXCHANGE SHUFFLE[1]
                    AGGREGATE ([LOCAL] aggregate [{18: sum=sum(17: expr)}] group by [[1: ps_partkey]] having [null]
                        SCAN (mv[partsupp_mv] columns[44: n_name, 48: ps_partkey, 58: ps_partvalue] predicate[44: n_name = PERU])
            EXCHANGE BROADCAST
                ASSERT LE 1
                    AGGREGATE ([GLOBAL] aggregate [{36: sum=sum(36: sum)}] group by [[]] having [null]
                        EXCHANGE GATHER
                            AGGREGATE ([LOCAL] aggregate [{36: sum=sum(35: expr)}] group by [[]] having [null]
                                SCAN (mv[partsupp_mv] columns[114: n_name, 128: ps_partvalue] predicate[114: n_name = PERU])
[end]

