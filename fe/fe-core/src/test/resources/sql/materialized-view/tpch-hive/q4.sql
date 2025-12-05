[sql]
select
    o_orderpriority,
    count(*) as order_count
from
    hive0.tpch.orders
where
        o_orderdate >= date '1994-09-01'
  and o_orderdate < date '1994-12-01'
  and exists (
        select
            *
        from
            hive0.tpch.lineitem
        where
                l_orderkey = o_orderkey
          and l_receiptdate > l_commitdate
    )
group by
    o_orderpriority
order by
    o_orderpriority ;
[result]
TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
    TOP-N (order by [[6: o_orderpriority ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{107: count=sum(107: count)}] group by [[29: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[29]
                AGGREGATE ([LOCAL] aggregate [{107: count=sum(30: order_count)}] group by [[29: o_orderpriority]] having [null]
                    SCAN (mv[query4_mv] columns[28: o_orderdate, 29: o_orderpriority, 30: order_count] predicate[28: o_orderdate >= 1994-09-01 AND 28: o_orderdate < 1994-12-01])
[end]

