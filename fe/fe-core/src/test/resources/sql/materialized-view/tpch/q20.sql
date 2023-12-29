[result]
TOP-N (order by [[2: s_name ASC NULLS FIRST]])
    TOP-N (order by [[2: s_name ASC NULLS FIRST]])
        LEFT SEMI JOIN (join-predicate [1: s_suppkey = 13: ps_suppkey] post-join-predicate [null])
            INNER JOIN (join-predicate [4: s_nationkey = 8: n_nationkey] post-join-predicate [null])
                SCAN (table[supplier] columns[1: s_suppkey, 2: s_name, 3: s_address, 4: s_nationkey] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (table[nation] columns[8: n_nationkey, 9: n_name] predicate[9: n_name = ARGENTINA])
            EXCHANGE SHUFFLE[13]
                INNER JOIN (join-predicate [30: l_partkey = 12: ps_partkey AND 31: l_suppkey = 13: ps_suppkey AND cast(14: ps_availqty as decimal128(38, 3)) > multiply(0.5, 43: sum)] post-join-predicate [null])
                    AGGREGATE ([GLOBAL] aggregate [{144: sum=sum(144: sum)}] group by [[131: l_partkey, 129: l_suppkey]] having [null]
                        EXCHANGE SHUFFLE[131, 129]
                            AGGREGATE ([LOCAL] aggregate [{144: sum=sum(132: sum_qty)}] group by [[131: l_partkey, 129: l_suppkey]] having [null]
                                SCAN (mv[lineitem_agg_mv2] columns[129: l_suppkey, 130: l_shipdate, 131: l_partkey, 132: sum_qty] predicate[130: l_shipdate >= 1993-01-01 AND 130: l_shipdate < 1994-01-01])
                    EXCHANGE SHUFFLE[12, 13]
                        LEFT SEMI JOIN (join-predicate [12: ps_partkey = 17: p_partkey] post-join-predicate [null])
                            SCAN (table[partsupp] columns[12: ps_partkey, 13: ps_suppkey, 14: ps_availqty] predicate[null])
                            EXCHANGE SHUFFLE[17]
                                SCAN (table[part] columns[17: p_partkey, 18: p_name] predicate[18: p_name LIKE sienna%])
[end]

