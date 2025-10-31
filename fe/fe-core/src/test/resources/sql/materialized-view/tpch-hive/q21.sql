[sql]
select
    s_name,
    count(*) as numwait
from
    hive0.tpch.supplier,
    hive0.tpch.lineitem l1,
    hive0.tpch.orders,
    hive0.tpch.nation
where
        s_suppkey = l1.l_suppkey
  and o_orderkey = l1.l_orderkey
  and o_orderstatus = 'F'
  and l1.l_receiptdate > l1.l_commitdate
  and exists (
        select
            *
        from
            hive0.tpch.lineitem l2
        where
                l2.l_orderkey = l1.l_orderkey
          and l2.l_suppkey <> l1.l_suppkey
    )
  and not exists (
        select
            *
        from
            hive0.tpch.lineitem l3
        where
                l3.l_orderkey = l1.l_orderkey
          and l3.l_suppkey <> l1.l_suppkey
          and l3.l_receiptdate > l3.l_commitdate
    )
  and s_nationkey = n_nationkey
  and n_name = 'CANADA'
group by
    s_name
order by
    numwait desc,
    s_name limit 100;
[result]
TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
    TOP-N (order by [[71: count DESC NULLS LAST, 2: s_name ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{71: count=count(71: count)}] group by [[2: s_name]] having [null]
            EXCHANGE SHUFFLE[2]
                AGGREGATE ([LOCAL] aggregate [{71: count=count()}] group by [[2: s_name]] having [null]
                    RIGHT SEMI JOIN (join-predicate [37: l_orderkey = 8: l_orderkey AND 39: l_suppkey != 10: l_suppkey] post-join-predicate [null])
                        EXCHANGE SHUFFLE[37]
                            HIVE SCAN (columns{37,39} predicate[37: l_orderkey IS NOT NULL])
                        RIGHT ANTI JOIN (join-predicate [54: l_orderkey = 8: l_orderkey AND 56: l_suppkey != 10: l_suppkey] post-join-predicate [null])
                            EXCHANGE SHUFFLE[54]
                                HIVE SCAN (columns{54,56,65,66} predicate[66: l_receiptdate > 65: l_commitdate])
                            EXCHANGE SHUFFLE[8]
                                SCAN (mv[lineitem_mv] columns[116: c_nationkey, 118: l_commitdate, 121: l_orderkey, 124: l_receiptdate, 129: l_suppkey, 133: o_orderstatus, 141: s_name, 142: s_nationkey, 150: n_name2] predicate[142: s_nationkey = 116: c_nationkey AND 133: o_orderstatus = F AND 150: n_name2 = CANADA AND 118: l_commitdate < 124: l_receiptdate])
[end]

