[sql]
select
  o_orderpriority,
  count(*) as order_count
from
  orders
where
  o_orderdate >= date '1993-07-01'
  and o_orderdate < date '1993-07-01' + interval '3' month
  and exists (
    select
      *
    from
      lineitem
    where
      l_orderkey = o_orderkey
      and l_commitdate < l_receiptdate
  )
group by
  o_orderpriority
order by
  o_orderpriority;
[result]
TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
    TOP-N (order by [[6: O_ORDERPRIORITY ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{29: count=count(29: count)}] group by [[6: O_ORDERPRIORITY]] having [null]
            EXCHANGE SHUFFLE[6]
                AGGREGATE ([LOCAL] aggregate [{29: count=count()}] group by [[6: O_ORDERPRIORITY]] having [null]
                    LEFT SEMI JOIN (join-predicate [1: O_ORDERKEY = 11: L_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[1: O_ORDERKEY, 5: O_ORDERDATE, 6: O_ORDERPRIORITY] predicate[5: O_ORDERDATE >= 1993-07-01 AND 5: O_ORDERDATE <= 1993-09-30])
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[22: L_COMMITDATE, 23: L_RECEIPTDATE, 11: L_ORDERKEY] predicate[22: L_COMMITDATE < 23: L_RECEIPTDATE])
[end]