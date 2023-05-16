[sql]
select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
        p_partkey = ps_partkey
  and s_suppkey = ps_suppkey
  and p_size = 12
  and p_type like '%COPPER'
  and s_nationkey = n_nationkey
  and n_regionkey = r_regionkey
  and r_name = 'AMERICA'
  and ps_supplycost = (
    select
        min(ps_supplycost)
    from
        partsupp,
        supplier,
        nation,
        region
    where
            p_partkey = ps_partkey
      and s_suppkey = ps_suppkey
      and s_nationkey = n_nationkey
      and n_regionkey = r_regionkey
      and r_name = 'AMERICA'
)
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey limit 100;
[result]
TOP-N (order by [[15: s_acctbal DESC NULLS LAST, 23: n_name ASC NULLS FIRST, 11: s_name ASC NULLS FIRST, 1: p_partkey ASC NULLS FIRST]])
    TOP-N (order by [[15: s_acctbal DESC NULLS LAST, 23: n_name ASC NULLS FIRST, 11: s_name ASC NULLS FIRST, 1: p_partkey ASC NULLS FIRST]])
        PREDICATE 20: ps_supplycost = 121: min
            ANALYTIC ({121: min=min(20: ps_supplycost)} [1: p_partkey] [] )
                TOP-N (order by [[1: p_partkey ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[1]
                        SCAN (mv[partsupp_mv] columns[106: n_name, 107: p_mfgr, 108: p_size, 109: p_type, 110: ps_partkey, 112: ps_supplycost, 113: r_name, 114: s_acctbal, 115: s_address, 116: s_comment, 117: s_name, 119: s_phone] predicate[108: p_size = 12 AND 113: r_name = AMERICA AND 109: p_type LIKE %COPPER])
[end]

