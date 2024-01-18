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
TOP-N (order by [[39: sum DESC NULLS LAST]])
    TOP-N (order by [[39: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{39: sum=sum(39: sum)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
            EXCHANGE SHUFFLE[1, 2, 6, 5, 35, 3, 8]
                AGGREGATE ([LOCAL] aggregate [{39: sum=sum(38: expr)}] group by [[1: c_custkey, 2: c_name, 6: c_acctbal, 5: c_phone, 35: n_name, 3: c_address, 8: c_comment]] having [null]
<<<<<<< HEAD
                    SCAN (mv[lineitem_mv] columns[51: c_address, 52: c_acctbal, 53: c_comment, 55: c_name, 57: c_phone, 65: l_returnflag, 70: o_custkey, 71: o_orderdate, 84: l_saleprice, 90: n_name2] predicate[71: o_orderdate >= 1994-05-01 AND 71: o_orderdate < 1994-08-01 AND 65: l_returnflag = R])
=======
                    SCAN (mv[lineitem_mv] columns[109: c_address, 110: c_acctbal, 111: c_comment, 113: c_name, 115: c_phone, 123: l_returnflag, 128: o_custkey, 129: o_orderdate, 142: l_saleprice, 148: n_name2] predicate[129: o_orderdate >= 1994-05-01 AND 129: o_orderdate < 1994-08-01 AND 123: l_returnflag = R AND 129: o_orderdate >= 1994-01-01 AND 129: o_orderdate < 1995-01-01])
>>>>>>> branch-2.5-mrs
[end]

