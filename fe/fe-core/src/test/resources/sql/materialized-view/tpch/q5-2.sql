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
group by
    n_name
order by
    revenue desc ;
[result]
TOP-N (order by [[: sum DESC NULLS LAST]])
    TOP-N (order by [[: sum DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{: sum=sum(: sum)}] group by [[: n_name]] having [null]
            EXCHANGE SHUFFLE[]
                AGGREGATE ([LOCAL] aggregate [{: sum=sum(: expr)}] group by [[: n_name]] having [null]
                    SCAN (mv[lineitem_mv] columns[: c_nationkey, : s_nationkey, : l_saleprice, : n_name] predicate[: s_nationkey = : c_nationkey])
[end]

