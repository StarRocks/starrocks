[sql]
select
            n1.n_name as supp_nation,
            n2.n_name as cust_nation,
            extract(year from l_shipdate) as l_year,
            l_extendedprice * (1 - l_discount) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
                s_suppkey = l_suppkey
          and o_orderkey = l_orderkey
          and c_custkey = o_custkey
          and s_nationkey = n1.n_nationkey
          and c_nationkey = n2.n_nationkey
          and (
                (n1.n_name = 'CANADA' and n2.n_name = 'IRAN')
                or (n1.n_name = 'IRAN' and n2.n_name = 'CANADA')
            );
[result]
SCAN (mv[lineitem_mv] columns[131: l_shipyear, 132: l_saleprice, 136: n_name1, 138: n_name2] predicate[136: n_name1 = CANADA AND 138: n_name2 = IRAN OR 136: n_name1 = IRAN AND 138: n_name2 = CANADA AND 136: n_name1 IN (CANADA, IRAN) AND 138: n_name2 IN (IRAN, CANADA)])
[end]

