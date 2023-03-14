[sql]
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    customer,
    orders,
    lineitem
where
  c_mktsegment = 'HOUSEHOLD'
  and c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate < date '1995-03-11'
  and l_shipdate > date '1995-03-11'
group by
    l_orderkey,
    o_orderdate,
    o_shippriority
order by
    revenue desc,
    o_orderdate limit 10;
[result]
TOP-N (order by [[35: sum DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[35: sum DESC NULLS LAST, 10: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[19: l_orderkey, 10: o_orderdate, 16: o_shippriority]] having [null]
            EXCHANGE SHUFFLE[19, 10, 16]
                AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[19: l_orderkey, 10: o_orderdate, 16: o_shippriority]] having [null]
                    SCAN (mv[lineitem_mv] columns[72: c_mktsegment, 79: l_orderkey, 84: l_shipdate, 89: o_orderdate, 92: o_shippriority, 102: l_saleprice] predicate[72: c_mktsegment = HOUSEHOLD AND 89: o_orderdate < 1995-03-11 AND 84: l_shipdate > 1995-03-11 AND 89: o_orderdate >= 1992-01-01 AND 89: o_orderdate < 1996-01-01 AND 84: l_shipdate >= 1995-01-01 AND 84: l_shipdate < 1999-01-01])
[end]

