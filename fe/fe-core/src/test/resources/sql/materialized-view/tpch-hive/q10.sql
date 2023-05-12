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
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem,
    hive0.tpch.nation
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
TOP-N (order by [[39: sum DESC NULLS LAST]])
    TOP-N (order by [[39: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{39: sum=sum(39: sum)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 35, 3, 8]
                AGGREGATE ([LOCAL] aggregate [{39: sum=sum(38: expr)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
                    SCAN (mv[lineitem_mv] columns[60: c_address, 61: c_acctbal, 62: c_comment, 64: c_name, 66: c_phone, 74: l_returnflag, 79: o_custkey, 80: o_orderdate, 93: l_saleprice, 99: n_name2] predicate[80: o_orderdate >= 1994-05-01 AND 80: o_orderdate < 1994-08-01 AND 74: l_returnflag = R])
[end]

