[sql]
select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
        c_custkey = o_custkey
  and l_orderkey = o_orderkey
  and o_orderdate >= date '1994-05-01'
  and o_orderdate < date '1994-08-01'
  and l_returnflag = 'R'
  and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc limit 20;
[plan-1]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-2]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-3]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[11]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                EXCHANGE SHUFFLE[1]
                                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-4]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-5]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-6]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-7]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-8]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-9]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                EXCHANGE SHUFFLE[20]
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-10]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                EXCHANGE BROADCAST
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-11]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE SHUFFLE[11]
                            INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                EXCHANGE SHUFFLE[10]
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-12]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-13]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-14]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                INNER JOIN (join-predicate [11: O_CUSTKEY = 1: C_CUSTKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[11]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                    EXCHANGE SHUFFLE[1]
                                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-15]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-16]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-17]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-18]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-19]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-20]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                                    EXCHANGE SHUFFLE[20]
                                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-21]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                    EXCHANGE BROADCAST
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-22]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    EXCHANGE SHUFFLE[4]
                        INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                            SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                            EXCHANGE SHUFFLE[11]
                                INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                                    SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                                    EXCHANGE SHUFFLE[10]
                                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                    EXCHANGE SHUFFLE[37]
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
[end]
[plan-23]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-24]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-25]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]
[plan-26]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]
[plan-27]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE SHUFFLE[11]
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-28]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE SHUFFLE[11]
                        INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                            EXCHANGE SHUFFLE[20]
                                SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-29]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE SHUFFLE[11]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                            EXCHANGE BROADCAST
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]
[plan-30]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 38, 3, 8]
                INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                    INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                        SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                        EXCHANGE BROADCAST
                            SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                    EXCHANGE SHUFFLE[11]
                        INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                            EXCHANGE SHUFFLE[10]
                                SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]
[plan-31]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                EXCHANGE SHUFFLE[11]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-32]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                EXCHANGE SHUFFLE[11]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
[end]
[plan-33]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                EXCHANGE SHUFFLE[11]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                        EXCHANGE BROADCAST
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]
[plan-34]
TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
    TOP-N (order by [[43: sum(42: expr) DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum(42: expr)=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                INNER JOIN (join-predicate [4: C_NATIONKEY = 37: N_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[1: C_CUSTKEY, 2: C_NAME, 3: C_ADDRESS, 4: C_NATIONKEY, 5: C_PHONE, 6: C_ACCTBAL, 8: C_COMMENT] predicate[null])
                    EXCHANGE BROADCAST
                        SCAN (columns[37: N_NATIONKEY, 38: N_NAME] predicate[null])
                EXCHANGE SHUFFLE[11]
                    INNER JOIN (join-predicate [20: L_ORDERKEY = 10: O_ORDERKEY] post-join-predicate [null])
                        SCAN (columns[20: L_ORDERKEY, 25: L_EXTENDEDPRICE, 26: L_DISCOUNT, 28: L_RETURNFLAG] predicate[28: L_RETURNFLAG = R])
                        EXCHANGE SHUFFLE[10]
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 14: O_ORDERDATE] predicate[14: O_ORDERDATE >= 1994-05-01 AND 14: O_ORDERDATE < 1994-08-01])
[end]