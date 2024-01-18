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
[result]
<<<<<<< HEAD
TOP-N (order by [[: n_name ASC NULLS FIRST, : year DESC NULLS LAST]])
    TOP-N (order by [[: n_name ASC NULLS FIRST, : year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{: sum=sum(: sum)}] group by [[: n_name, : year]] having [null]
            EXCHANGE SHUFFLE[, ]
                AGGREGATE ([LOCAL] aggregate [{: sum=sum(: expr)}] group by [[: n_name, : year]] having [null]
SCAN (mv[lineitem_mv] columns[: c_nationkey, : p_name, : s_nationkey, : l_amount, : o_orderyear, : n_name] predicate[: c_nationkey = : s_nationkey AND : p_name LIKE %peru%])[end]
=======
TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
    TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
        AGGREGATE ([GLOBAL] aggregate [{53: sum=sum(53: sum)}] group by [[48: n_name, 51: year]] having [null]
            EXCHANGE SHUFFLE[48, 51]
                AGGREGATE ([LOCAL] aggregate [{53: sum=sum(52: expr)}] group by [[48: n_name, 51: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[122: c_nationkey, 144: p_name, 148: s_nationkey, 151: l_amount, 153: o_orderyear, 156: n_name2] predicate[148: s_nationkey = 122: c_nationkey AND 144: p_name LIKE %peru%])
[end]
>>>>>>> branch-2.5-mrs

