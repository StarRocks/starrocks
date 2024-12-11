<<<<<<< HEAD
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
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
[result]
TOP-N (order by [[49: sum DESC NULLS LAST]])
    TOP-N (order by [[49: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{49: sum=sum(49: sum)}] group by [[42: n_name]] having [null]
            EXCHANGE SHUFFLE[42]
                AGGREGATE ([LOCAL] aggregate [{49: sum=sum(48: expr)}] group by [[42: n_name]] having [null]
<<<<<<< HEAD
                    SCAN (mv[lineitem_mv] columns[111: c_nationkey, 126: o_orderdate, 137: s_nationkey, 139: l_saleprice, 145: n_name2, 148: r_name2] predicate[137: s_nationkey = 111: c_nationkey AND 126: o_orderdate >= 1995-01-01 AND 126: o_orderdate < 1996-01-01 AND 148: r_name2 = AFRICA])
[end]

=======
                    SCAN (mv[lineitem_mv] columns[76: c_nationkey, 91: o_orderdate, 102: s_nationkey, 104: l_saleprice, 110: n_name2, 113: r_name2] predicate[102: s_nationkey = 76: c_nationkey AND 91: o_orderdate >= 1995-01-01 AND 91: o_orderdate < 1996-01-01 AND 113: r_name2 = AFRICA])
[end]
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
