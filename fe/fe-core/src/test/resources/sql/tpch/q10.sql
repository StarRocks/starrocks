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
[result]
TOP-N (order by [[43: sum DESC NULLS LAST]])
    TOP-N (order by [[43: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{43: sum=sum(42: expr)}] group by [[1: C_CUSTKEY, 2: C_NAME, 6: C_ACCTBAL, 5: C_PHONE, 38: N_NAME, 3: C_ADDRESS, 8: C_COMMENT]] having [null]
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

