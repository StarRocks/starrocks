[sql]
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and l_suppkey = s_suppkey
  and c_nationkey = s_nationkey
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AFRICA'
  and o_orderdate >= date '1995-01-01'
  and o_orderdate < date '1996-01-01'
group by
    n_name
order by
    revenue desc ;
[plan-1]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[20]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-2]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[20]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-3]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[20]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-4]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[20]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-5]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-6]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-7]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-8]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-9]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-10]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-11]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-12]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-13]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-14]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-15]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-16]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-17]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-18]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-19]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[47]
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[50]
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-20]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[47]
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[50]
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-21]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-22]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-23]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[47]
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[50]
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-24]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[40]
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                EXCHANGE SHUFFLE[45]
                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[47]
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[50]
                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE BROADCAST
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-25]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-26]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-27]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-28]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-29]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-30]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-31]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-32]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[11, 40]
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                    EXCHANGE SHUFFLE[1, 4]
                        SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-33]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-34]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-35]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-36]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[20]
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-37]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-38]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-39]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-40]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-41]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[20]
                                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-42]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[20]
                                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                        EXCHANGE SHUFFLE[47]
                                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                        EXCHANGE SHUFFLE[50]
                                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-43]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[20]
                                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[40]
                                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[45]
                                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                        EXCHANGE BROADCAST
                                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-44]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[20]
                                    INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                        SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[40]
                                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[45]
                                                    INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                        EXCHANGE SHUFFLE[47]
                                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                        EXCHANGE SHUFFLE[50]
                                                            SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-45]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-46]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-47]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE BROADCAST
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-48]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[40]
                                                SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[45]
                                                INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                    EXCHANGE SHUFFLE[47]
                                                        SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                    EXCHANGE SHUFFLE[50]
                                                        SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-49]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-50]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-51]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-52]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-53]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-54]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE BROADCAST
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-55]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-56]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[40]
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[45]
                                        INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                            EXCHANGE SHUFFLE[47]
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                            EXCHANGE SHUFFLE[50]
                                                SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE BROADCAST
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-57]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-58]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-59]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-60]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE BROADCAST
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-61]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-62]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE BROADCAST
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-63]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]
[plan-64]
TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
    TOP-N (order by [[55: sum(54: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{55: sum(54: expr)=sum(55: sum(54: expr))}] group by [[46: N_NAME]] having [null]
            EXCHANGE SHUFFLE[46]
                AGGREGATE ([LOCAL] aggregate [{55: sum(54: expr)=sum(54: expr)}] group by [[46: N_NAME]] having [null]
                    INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY AND 40: S_NATIONKEY = 4: C_NATIONKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[11, 40]
                            INNER JOIN (join-predicate [22: L_SUPPKEY = 37: S_SUPPKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 22: L_SUPPKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT] predicate[null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1995-01-01 AND 14: O_ORDERDATE < 1996-01-01])
                                EXCHANGE BROADCAST
                                    INNER JOIN (join-predicate [40: S_NATIONKEY = 45: N_NATIONKEY] post-join-predicate [null])
                                        EXCHANGE SHUFFLE[40]
                                            SCAN (columns[37: S_SUPPKEY, 40: S_NATIONKEY] predicate[null])
                                        EXCHANGE SHUFFLE[45]
                                            INNER JOIN (join-predicate [47: N_REGIONKEY = 50: R_REGIONKEY] post-join-predicate [null])
                                                EXCHANGE SHUFFLE[47]
                                                    SCAN (columns[45: N_NATIONKEY, 46: N_NAME, 47: N_REGIONKEY] predicate[null])
                                                EXCHANGE SHUFFLE[50]
                                                    SCAN (columns[50: R_REGIONKEY, 51: R_NAME] predicate[51: R_NAME = AFRICA])
                        EXCHANGE SHUFFLE[1, 4]
                            SCAN (columns[1: C_CUSTKEY, 4: C_NATIONKEY] predicate[null])
[end]