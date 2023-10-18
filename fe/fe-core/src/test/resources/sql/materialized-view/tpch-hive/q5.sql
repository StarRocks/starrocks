[sql]
select
    n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
from
    hive0.tpch.customer,
    hive0.tpch.orders,
    hive0.tpch.lineitem,
    hive0.tpch.supplier,
    hive0.tpch.nation,
    hive0.tpch.region
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
[result]
TOP-N (order by [[49: sum DESC NULLS LAST]])
    TOP-N (order by [[49: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum)}] group by [[42: n_name]] having [null]
            EXCHANGE SHUFFLE[42]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(48: expr)}] group by [[42: n_name]] having [null]
                    SCAN (mv[lineitem_mv] columns[112: c_nationkey, 127: o_orderdate, 138: s_nationkey, 140: l_saleprice, 146: n_name2, 149: r_name2] predicate[138: s_nationkey = 112: c_nationkey AND 127: o_orderdate >= 1995-01-01 AND 127: o_orderdate < 1996-01-01 AND 149: r_name2 = AFRICA])
[end]

