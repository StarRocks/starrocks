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
[plan-1]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                    EXCHANGE SHUFFLE[20]
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-2]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                    EXCHANGE SHUFFLE[20]
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-3]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                    EXCHANGE SHUFFLE[20]
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-4]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                    EXCHANGE SHUFFLE[20]
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-5]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
[plan-6]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-7]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-8]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
[plan-9]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[1]
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-10]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                            EXCHANGE SHUFFLE[11]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-11]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                    EXCHANGE SHUFFLE[10]
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
[plan-12]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-13]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-14]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-15]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                            EXCHANGE BROADCAST
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
[end]
[plan-16]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
[plan-17]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-18]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-19]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
[plan-20]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-21]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
[end]
[plan-22]
TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[38: sum(37: expr) DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{38: sum(37: expr)=sum(38: sum(37: expr))}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
            EXCHANGE SHUFFLE[20, 14, 17]
                AGGREGATE ([LOCAL] aggregate [{38: sum(37: expr)=sum(37: expr)}] group by [[20: L_ORDERKEY, 14: O_ORDERDATE, 17: O_SHIPPRIORITY]] having [null]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 30: L_SHIPDATE] predicate[30: L_SHIPDATE > 1995-03-11])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[17: O_SHIPPRIORITY, 10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE < 1995-03-11])
                                EXCHANGE BROADCAST
                                    SCAN (columns[1: C_CUSTKEY, 7: C_MKTSEGMENT] predicate[7: C_MKTSEGMENT = HOUSEHOLD])
[end]
