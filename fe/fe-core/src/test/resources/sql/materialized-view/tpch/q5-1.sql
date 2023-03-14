[sql]
select
    count(*)
from
    customer,
    orders,
    lineitem,
    supplier
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey;
[result]
AGGREGATE ([GLOBAL] aggregate [{41: count=count(41: count)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{41: count=count()}] group by [[]] having [null]
            SCAN (mv[lineitem_mv] columns[95: c_nationkey, 121: s_nationkey] predicate[121: s_nationkey = 95: c_nationkey])
[end]

