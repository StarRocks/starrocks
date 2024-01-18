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
        AGGREGATE ([GLOBAL] aggregate [{113: count=sum(113: count)}] group by [[81: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[81]
                AGGREGATE ([LOCAL] aggregate [{113: count=sum(82: order_count)}] group by [[81: o_orderpriority]] having [null]
                    SCAN (mv[query4_mv] columns[80: o_orderdate, 81: o_orderpriority, 82: order_count] predicate[80: o_orderdate >= 1994-09-01 AND 80: o_orderdate < 1994-12-01])
[end]

