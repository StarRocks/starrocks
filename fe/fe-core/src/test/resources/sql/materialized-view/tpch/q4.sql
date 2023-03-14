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
        AGGREGATE ([GLOBAL] aggregate [{128: count=sum(128: count)}] group by [[112: o_orderpriority]] having [null]
            EXCHANGE SHUFFLE[112]
                AGGREGATE ([LOCAL] aggregate [{128: count=sum(113: order_count)}] group by [[112: o_orderpriority]] having [null]
                    SCAN (mv[query4_mv] columns[111: o_orderdate, 112: o_orderpriority, 113: order_count] predicate[111: o_orderdate >= 1994-09-01 AND 111: o_orderdate < 1994-12-01 AND 111: o_orderdate >= 1994-01-01 AND 111: o_orderdate < 1995-01-01])
[end]

