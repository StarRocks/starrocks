[sql]
select
    nation,
    o_year,
    sum(amount) as sum_profit
from
    (
        select
            n_name as nation,
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
        from
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        where
                s_suppkey = l_suppkey
          and ps_suppkey = l_suppkey
          and ps_partkey = l_partkey
          and p_partkey = l_partkey
          and o_orderkey = l_orderkey
          and s_nationkey = n_nationkey
          and p_name like '%peru%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc ;
[plan-1]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-2]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-3]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-4]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-5]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-6]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-7]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-8]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-9]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-10]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-11]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-12]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [37: PS_SUPPKEY = 21: L_SUPPKEY AND 36: PS_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE BROADCAST
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-13]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-14]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-15]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-16]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-17]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-18]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[14]
                                        SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE SHUFFLE[52]
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [1: P_PARTKEY = 20: L_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])
[plan-19]
TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
    TOP-N (order by [[53: N_NAME ASC NULLS FIRST, 57: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{59: sum=sum(59: sum)}] group by [[53: N_NAME, 57: year]] having [null]
            EXCHANGE SHUFFLE[53, 57]
                AGGREGATE ([LOCAL] aggregate [{59: sum=sum(58: expr)}] group by [[53: N_NAME, 57: year]] having [null]
                    INNER JOIN (join-predicate [11: S_SUPPKEY = 21: L_SUPPKEY AND 42: O_ORDERKEY = 19: L_ORDERKEY] post-join-predicate [null])
                        CROSS JOIN (join-predicate [null] post-join-predicate [null])
                            SCAN (columns[42: O_ORDERKEY, 46: O_ORDERDATE] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [14: S_NATIONKEY = 52: N_NATIONKEY] post-join-predicate [null])
                                    SCAN (columns[11: S_SUPPKEY, 14: S_NATIONKEY] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[52: N_NATIONKEY, 53: N_NAME] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [21: L_SUPPKEY = 37: PS_SUPPKEY AND 20: L_PARTKEY = 36: PS_PARTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[21, 20]
                                    INNER JOIN (join-predicate [20: L_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                        SCAN (columns[19: L_ORDERKEY, 20: L_PARTKEY, 21: L_SUPPKEY, 23: L_QUANTITY, 24: L_EXTENDEDPRICE, 25: L_DISCOUNT] predicate[null])
                                        EXCHANGE BROADCAST
                                            SCAN (columns[1: P_PARTKEY, 2: P_NAME] predicate[2: P_NAME LIKE %peru%])
                                EXCHANGE SHUFFLE[37, 36]
                                    SCAN (columns[36: PS_PARTKEY, 37: PS_SUPPKEY, 39: PS_SUPPLYCOST] predicate[null])