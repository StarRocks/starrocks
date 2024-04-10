[result]
TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
    TOP-N (order by [[1: s_suppkey ASC NULLS FIRST]])
        INNER JOIN (join-predicate [1: s_suppkey = 12: l_suppkey] post-join-predicate [null])
            SCAN (table[supplier] columns[1: s_suppkey, 2: s_name, 3: s_address, 5: s_phone] predicate[null])
            EXCHANGE SHUFFLE[12]
                INNER JOIN (join-predicate [25: sum = 44: max] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{148: sum=sum(148: sum)}] group by [[64: l_suppkey]] having [148: sum IS NOT NULL]
                        EXCHANGE SHUFFLE[64]
                            AGGREGATE ([LOCAL] aggregate [{148: sum=sum(73: sum_disc_price)}] group by [[64: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv2] columns[64: l_suppkey, 65: l_shipdate, 73: sum_disc_price] predicate[65: l_shipdate >= 1995-07-01 AND 65: l_shipdate < 1995-10-01])
                    EXCHANGE BROADCAST
                        PREDICATE 44: max IS NOT NULL
                            ASSERT LE 1
                                AGGREGATE ([GLOBAL] aggregate [{44: max=max(44: max)}] group by [[]] having [null]
                                    EXCHANGE GATHER
                                        AGGREGATE ([LOCAL] aggregate [{44: max=max(43: sum)}] group by [[]] having [null]
                                            AGGREGATE ([GLOBAL] aggregate [{149: sum=sum(149: sum)}] group by [[64: l_suppkey]] having [null]
                                                EXCHANGE SHUFFLE[64]
                                                    AGGREGATE ([LOCAL] aggregate [{149: sum=sum(73: sum_disc_price)}] group by [[64: l_suppkey]] having [null]
                                                        SCAN (mv[lineitem_agg_mv2] columns[64: l_suppkey, 65: l_shipdate, 73: sum_disc_price] predicate[65: l_shipdate >= 1995-07-01 AND 65: l_shipdate < 1995-10-01])
[end]

