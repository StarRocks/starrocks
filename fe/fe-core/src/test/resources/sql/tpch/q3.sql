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
TOP-N (order by [[38: sum DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                EXCHANGE SHUFFLE[10]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]

