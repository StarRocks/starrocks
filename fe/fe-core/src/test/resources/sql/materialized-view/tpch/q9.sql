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
        AGGREGATE ([GLOBAL] aggregate [{1286: sum=sum(1286: sum)}] group by [[99: n_name2, 98: o_orderyear]] having [null]
            EXCHANGE SHUFFLE[99, 98]
                AGGREGATE ([LOCAL] aggregate [{1286: sum=sum(100: sum_amount)}] group by [[99: n_name2, 98: o_orderyear]] having [null]
                    SCAN (mv[lineitem_mv_agg_mv1] columns[97: p_name, 98: o_orderyear, 99: n_name2, 100: sum_amount] predicate[97: p_name LIKE %peru%])
[end]

