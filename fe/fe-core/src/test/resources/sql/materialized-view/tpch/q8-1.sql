[sql]
select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
                p_partkey = l_partkey
          and s_suppkey = l_suppkey
          and l_orderkey = o_orderkey
          and o_custkey = c_custkey
          and c_nationkey = n1.n_nationkey
          and n1.n_regionkey = r_regionkey
          and s_nationkey = n2.n_nationkey
[result]
SCAN (mv[lineitem_mv] columns[144: l_saleprice, 147: o_orderyear, 148: n_name1, 149: n_regionkey1, 151: n_regionkey2] predicate[149: n_regionkey1 = 151: n_regionkey2])
[end]

