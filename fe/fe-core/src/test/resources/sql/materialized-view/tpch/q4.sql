[sql]
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
        o_orderdate >= date '1994-09-01'
  and o_orderdate < date '1994-12-01'
  and exists (
        select
            *
        from
            lineitem
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
        AGGREGATE ([GLOBAL] aggregate [{115: count=sum(115: count)}] group by [[113: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[113]
                AGGREGATE ([LOCAL] aggregate [{115: count=sum(114: order_count)}] group by [[113: o_orderpriority]] having [null]
                    SCAN (mv[query4_mv] columns[112: o_orderdate, 113: o_orderpriority, 114: order_count] predicate[112: o_orderdate >= 1994-09-01 AND 112: o_orderdate < 1994-12-01])
[end]

