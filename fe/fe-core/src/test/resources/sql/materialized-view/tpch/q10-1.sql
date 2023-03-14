[sql]
select
    count(1)
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and c_nationkey = n_nationkey
[result]
AGGREGATE ([GLOBAL] aggregate [{38: count=count(38: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{38: count=count(1)}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[119: l_shipyear] predicate[null])
[end]

