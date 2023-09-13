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
                    SCAN (mv[lineitem_mv] columns[66: c_address, 67: c_acctbal, 68: c_comment, 70: c_name, 72: c_phone, 80: l_returnflag, 85: o_custkey, 86: o_orderdate, 99: l_saleprice, 105: n_name2] predicate[86: o_orderdate >= 1994-05-01 AND 86: o_orderdate < 1994-08-01 AND 80: l_returnflag = R])
[end]

