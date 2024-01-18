[sql]
select
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    o_orderdate,
    o_shippriority
from
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem
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
TOP-N (order by [[35: sum DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
    TOP-N (order by [[35: sum DESC NULLS LAST, 13: o_orderdate ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{35: sum=sum(35: sum)}] group by [[18: l_orderkey, 13: o_orderdate, 16: o_shippriority]] having [null]
            EXCHANGE SHUFFLE[18, 13, 16]
                AGGREGATE ([LOCAL] aggregate [{35: sum=sum(34: expr)}] group by [[18: l_orderkey, 13: o_orderdate, 16: o_shippriority]] having [null]
                    SCAN (mv[lineitem_mv] columns[44: c_mktsegment, 51: l_orderkey, 56: l_shipdate, 61: o_orderdate, 64: o_shippriority, 74: l_saleprice] predicate[61: o_orderdate < 1995-03-11 AND 56: l_shipdate > 1995-03-11 AND 44: c_mktsegment = HOUSEHOLD])
[end]

