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
    hive0.tpch.part,
    hive0.tpch.supplier,
    hive0.tpch.partsupp,
    hive0.tpch.nation,
    hive0.tpch.region
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
        hive0.tpch.partsupp,
        hive0.tpch.supplier,
        hive0.tpch.nation,
        hive0.tpch.region
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
        PREDICATE 20: ps_supplycost = 117: min
            ANALYTIC ({117: min=min(20: ps_supplycost)} [1: p_partkey] [] )
                TOP-N (order by [[1: p_partkey ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[1]
                        SCAN (mv[partsupp_mv] columns[50: n_name, 51: p_mfgr, 52: p_size, 53: p_type, 54: ps_partkey, 56: ps_supplycost, 57: r_name, 58: s_acctbal, 59: s_address, 60: s_comment, 61: s_name, 63: s_phone] predicate[57: r_name = AMERICA AND 52: p_size = 12 AND 53: p_type LIKE %COPPER])
[end]

