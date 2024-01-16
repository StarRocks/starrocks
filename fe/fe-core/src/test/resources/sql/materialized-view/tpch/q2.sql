[result]
TOP-N (order by [[15: s_acctbal DESC NULLS LAST, 23: n_name ASC NULLS FIRST, 11: s_name ASC NULLS FIRST, 1: p_partkey ASC NULLS FIRST]])
    TOP-N (order by [[15: s_acctbal DESC NULLS LAST, 23: n_name ASC NULLS FIRST, 11: s_name ASC NULLS FIRST, 1: p_partkey ASC NULLS FIRST]])
        PREDICATE 20: ps_supplycost = 121: min
            ANALYTIC ({121: min=min(20: ps_supplycost)} [1: p_partkey] [] )
                TOP-N (order by [[1: p_partkey ASC NULLS FIRST]])
                    EXCHANGE SHUFFLE[1]
                        SCAN (mv[partsupp_mv] columns[55: n_name, 56: p_mfgr, 57: p_size, 58: p_type, 59: ps_partkey, 61: ps_supplycost, 62: r_name, 63: s_acctbal, 64: s_address, 65: s_comment, 66: s_name, 68: s_phone] predicate[62: r_name = AMERICA AND 57: p_size = 12 AND 58: p_type LIKE %COPPER])
[end]

