[sql]
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
        o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 315
    )
  and c_custkey = o_custkey
  and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate limit 100;
[result]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum(24: L_QUANTITY)=sum(56: sum(24: L_QUANTITY))}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            EXCHANGE SHUFFLE[2, 1, 10, 14, 13]
                AGGREGATE ([LOCAL] aggregate [{56: sum(24: L_QUANTITY)=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                                EXCHANGE BROADCAST
                                    LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                                        EXCHANGE BROADCAST
                                            AGGREGATE ([GLOBAL] aggregate [{54: sum(41: L_QUANTITY)=sum(54: sum(41: L_QUANTITY))}] group by [[37: L_ORDERKEY]] having [54: sum(41: L_QUANTITY) > 315.0]
                                                AGGREGATE ([LOCAL] aggregate [{54: sum(41: L_QUANTITY)=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [null]
                                                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
[end]

