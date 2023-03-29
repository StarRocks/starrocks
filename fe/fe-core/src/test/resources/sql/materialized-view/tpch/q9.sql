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
TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
    TOP-N (order by [[48: n_name ASC NULLS FIRST, 51: year DESC NULLS LAST]])
<<<<<<< HEAD
        AGGREGATE ([GLOBAL] aggregate [{164: sum=sum(164: sum)}] group by [[122: n_name1, 121: o_orderyear]] having [null]
            EXCHANGE SHUFFLE[122, 121]
                AGGREGATE ([LOCAL] aggregate [{164: sum=sum(123: sum_amount)}] group by [[122: n_name1, 121: o_orderyear]] having [null]
                    SCAN (mv[lineitem_mv_agg_mv1] columns[120: p_name, 121: o_orderyear, 122: n_name1, 123: sum_amount] predicate[120: p_name LIKE %peru%])
=======
        AGGREGATE ([GLOBAL] aggregate [{53: sum=sum(53: sum)}] group by [[48: n_name, 51: year]] having [null]
            EXCHANGE SHUFFLE[48, 51]
                AGGREGATE ([LOCAL] aggregate [{53: sum=sum(52: expr)}] group by [[48: n_name, 51: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[101: c_nationkey, 123: p_name, 127: s_nationkey, 130: l_amount, 132: o_orderyear, 135: n_name2] predicate[127: s_nationkey = 101: c_nationkey AND 123: p_name LIKE %peru%])
>>>>>>> dd76ccdda ([Enhancement] Enhance mv rewrite when mv/query have multi same tables (#20263))
[end]

