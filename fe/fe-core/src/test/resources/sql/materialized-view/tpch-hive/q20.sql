[sql]
select
    s_name,
    s_address
from
    hive0.tpch.supplier,
    hive0.tpch.nation
where
        s_suppkey in (
        select
            ps_suppkey
        from
            hive0.tpch.partsupp
        where
                ps_partkey in (
                select
                    p_partkey
                from
                    hive0.tpch.part
                where
                        p_name like 'sienna%'
            )
          and ps_availqty > (
            select
                    0.5 * sum(l_quantity)
            from
                hive0.tpch.lineitem
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
        RIGHT SEMI JOIN (join-predicate [13: ps_suppkey = 1: s_suppkey] post-join-predicate [null])
            EXCHANGE SHUFFLE[13]
                INNER JOIN (join-predicate [12: ps_partkey = 28: l_partkey AND 13: ps_suppkey = 29: l_suppkey AND cast(14: ps_availqty as decimal128(38, 3)) > multiply(0.5, 43: sum)] post-join-predicate [null])
                    LEFT SEMI JOIN (join-predicate [12: ps_partkey = 17: p_partkey] post-join-predicate [null])
                        EXCHANGE SHUFFLE[12]
                            HIVE SCAN (columns{12,13,14} predicate[13: ps_suppkey IS NOT NULL])
                        EXCHANGE SHUFFLE[17]
                            HIVE SCAN (columns{17,18} predicate[17: p_partkey IS NOT NULL AND 18: p_name LIKE sienna%])
                    EXCHANGE SHUFFLE[28]
                        AGGREGATE ([GLOBAL] aggregate [{149: sum=sum(149: sum)}] group by [[132: l_partkey, 130: l_suppkey]] having [null]
                            EXCHANGE SHUFFLE[132, 130]
                                AGGREGATE ([LOCAL] aggregate [{149: sum=sum(133: sum_qty)}] group by [[132: l_partkey, 130: l_suppkey]] having [null]
                                    SCAN (mv[lineitem_agg_mv2] columns[130: l_suppkey, 131: l_shipdate, 132: l_partkey, 133: sum_qty] predicate[131: l_shipdate >= 1993-01-01 AND 131: l_shipdate < 1994-01-01])
            EXCHANGE SHUFFLE[1]
                INNER JOIN (join-predicate [4: s_nationkey = 8: n_nationkey] post-join-predicate [null])
                    HIVE SCAN (columns{1,2,3,4} predicate[4: s_nationkey IS NOT NULL])
                    EXCHANGE BROADCAST
                        HIVE SCAN (columns{8,9,147,148} predicate[9: n_name = ARGENTINA])
[end]

