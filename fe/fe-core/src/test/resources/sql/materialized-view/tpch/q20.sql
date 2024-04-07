[sql]
select
    s_name,
    s_address
from
    supplier,
    nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                        p_name like 'sienna%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                lineitem
            where
                    l_partkey = ps_partkey
              and l_suppkey = ps_suppkey
              and l_shipdate >= date '1993-01-01'
              and l_shipdate < date '1994-01-01'
        )
    )
  and s_nationkey = n_nationkey
  and n_name = 'ARGENTINA'
order by
    s_name ;
[result]
TOP-N (order by [[2: s_name ASC NULLS FIRST]])
    TOP-N (order by [[2: s_name ASC NULLS FIRST]])
        LEFT SEMI JOIN (join-predicate [1: s_suppkey = 13: ps_suppkey] post-join-predicate [null])
            INNER JOIN (join-predicate [4: s_nationkey = 8: n_nationkey] post-join-predicate [null])
                SCAN (table[supplier] columns[1: s_suppkey, 2: s_name, 3: s_address, 4: s_nationkey] predicate[null])
                EXCHANGE BROADCAST
                    SCAN (table[nation] columns[8: n_nationkey, 9: n_name] predicate[9: n_name = ARGENTINA])
            EXCHANGE SHUFFLE[13]
                INNER JOIN (join-predicate [12: ps_partkey = 30: l_partkey AND 13: ps_suppkey = 31: l_suppkey AND cast(14: ps_availqty as DECIMAL128(38,3)) > multiply(0.5, 43: sum)] post-join-predicate [null])
                    LEFT SEMI JOIN (join-predicate [12: ps_partkey = 17: p_partkey] post-join-predicate [null])
                        SCAN (table[partsupp] columns[12: ps_partkey, 13: ps_suppkey, 14: ps_availqty] predicate[null])
                        EXCHANGE SHUFFLE[17]
                            SCAN (table[part] columns[17: p_partkey, 18: p_name] predicate[18: p_name LIKE sienna%])
                    EXCHANGE SHUFFLE[30]
                        AGGREGATE ([GLOBAL] aggregate [{149: sum=sum(149: sum)}] group by [[67: l_partkey, 65: l_suppkey]] having [null]
                            EXCHANGE SHUFFLE[67, 65]
                                AGGREGATE ([LOCAL] aggregate [{149: sum=sum(68: sum_qty)}] group by [[67: l_partkey, 65: l_suppkey]] having [null]
                                    SCAN (mv[lineitem_agg_mv2] columns[65: l_suppkey, 66: l_shipdate, 67: l_partkey, 68: sum_qty] predicate[66: l_shipdate >= 1993-01-01 AND 66: l_shipdate < 1994-01-01])
[end]

