[sql]
select
    o_year,
    sum(case
            when nation = 'IRAN' then volume
            else 0
        end) / sum(volume) as mkt_share
from
    (
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
          and r_name = 'MIDDLE EAST'
          and s_nationkey = n2.n_nationkey
          and o_orderdate between date '1995-01-01' and date '1996-12-31'
          and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year ;
[result]
TOP-N (order by [[61: year ASC NULLS FIRST]])
    TOP-N (order by [[61: year ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{65: sum=sum(65: sum), 64: sum=sum(64: sum)}] group by [[61: year]] having [null]
            EXCHANGE SHUFFLE[61]
                AGGREGATE ([LOCAL] aggregate [{65: sum=sum(62: expr), 64: sum=sum(63: case)}] group by [[61: year]] having [null]
                    SCAN (mv[lineitem_mv] columns[87: o_orderdate, 96: p_type, 100: l_saleprice, 103: o_orderyear, 104: n_name1, 109: r_name2] predicate[109: r_name2 = MIDDLE EAST AND 87: o_orderdate <= 1996-12-31 AND 87: o_orderdate >= 1995-01-01 AND 96: p_type = ECONOMY ANODIZED STEEL])
[end]
