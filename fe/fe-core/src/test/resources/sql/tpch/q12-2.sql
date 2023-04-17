[sql]
select
  l_shipmode,
  sum(case
    when o_orderpriority = '1-URGENT'
      or o_orderpriority = '2-HIGH'
      then 1
    else 0
  end) as high_line_count,
  sum(case
    when o_orderpriority <> '1-URGENT'
      and o_orderpriority <> '2-HIGH'
      then 1
    else 0
  end) as low_line_count
from
  orders,
  lineitem
where
  o_orderkey = l_orderkey
  and l_shipmode in ('MAIL', 'SHIP')
  and l_commitdate < l_receiptdate
  and l_shipdate < l_commitdate
  and l_receiptdate >= date '1994-01-01'
  and l_receiptdate < date '1994-01-01' + interval '1' year
group by
  l_shipmode
order by
  l_shipmode;
[result]
TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
    TOP-N (order by [[25: L_SHIPMODE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{30: sum=sum(30: sum), 31: sum=sum(31: sum)}] group by [[25: L_SHIPMODE]] having [null]
            EXCHANGE SHUFFLE[25]
                AGGREGATE ([LOCAL] aggregate [{30: sum=sum(28: case), 31: sum=sum(29: case)}] group by [[25: L_SHIPMODE]] having [null]
                    INNER JOIN (join-predicate [1: O_ORDERKEY = 11: L_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[1: O_ORDERKEY, 6: O_ORDERPRIORITY] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            SCAN (columns[21: L_SHIPDATE, 22: L_COMMITDATE, 23: L_RECEIPTDATE, 25: L_SHIPMODE, 11: L_ORDERKEY] predicate[25: L_SHIPMODE IN (MAIL, SHIP) AND 22: L_COMMITDATE < 23: L_RECEIPTDATE AND 21: L_SHIPDATE < 22: L_COMMITDATE AND 23: L_RECEIPTDATE >= 1994-01-01 AND 23: L_RECEIPTDATE <= 1994-12-31])
[end]