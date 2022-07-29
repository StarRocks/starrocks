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
TOP-N (order by [[16: S_ACCTBAL DESC NULLS LAST, 26: N_NAME ASC NULLS FIRST, 12: S_NAME ASC NULLS FIRST, 1: P_PARTKEY ASC NULLS FIRST]])
    TOP-N (order by [[16: S_ACCTBAL DESC NULLS LAST, 26: N_NAME ASC NULLS FIRST, 12: S_NAME ASC NULLS FIRST, 1: P_PARTKEY ASC NULLS FIRST]])
        INNER JOIN (join-predicate [30: R_REGIONKEY = 27: N_REGIONKEY] post-join-predicate [null])
            SCAN (columns[30: R_REGIONKEY, 31: R_NAME] predicate[31: R_NAME = AMERICA])
            EXCHANGE BROADCAST
                INNER JOIN (join-predicate [25: N_NATIONKEY = 14: S_NATIONKEY] post-join-predicate [null])
                    SCAN (columns[25: N_NATIONKEY, 26: N_NAME, 27: N_REGIONKEY] predicate[null])
                    EXCHANGE BROADCAST
                        INNER JOIN (join-predicate [11: S_SUPPKEY = 20: PS_SUPPKEY] post-join-predicate [null])
                            SCAN (columns[17: S_COMMENT, 11: S_SUPPKEY, 12: S_NAME, 13: S_ADDRESS, 14: S_NATIONKEY, 15: S_PHONE, 16: S_ACCTBAL] predicate[null])
                            EXCHANGE BROADCAST
                                INNER JOIN (join-predicate [22: PS_SUPPLYCOST = 57: min AND 19: PS_PARTKEY = 1: P_PARTKEY] post-join-predicate [null])
                                    SCAN (columns[19: PS_PARTKEY, 20: PS_SUPPKEY, 22: PS_SUPPLYCOST] predicate[null])
                                    EXCHANGE SHUFFLE[1]
                                        INNER JOIN (join-predicate [1: P_PARTKEY = 34: PS_PARTKEY] post-join-predicate [null])
                                            SCAN (columns[1: P_PARTKEY, 3: P_MFGR, 5: P_TYPE, 6: P_SIZE] predicate[6: P_SIZE = 12 AND 5: P_TYPE LIKE %COPPER])
                                            EXCHANGE SHUFFLE[34]
                                                AGGREGATE ([GLOBAL] aggregate [{57: min=min(57: min)}] group by [[34: PS_PARTKEY]] having [57: min IS NOT NULL]
                                                    AGGREGATE ([LOCAL] aggregate [{57: min=min(37: PS_SUPPLYCOST)}] group by [[34: PS_PARTKEY]] having [null]
                                                        INNER JOIN (join-predicate [35: PS_SUPPKEY = 40: S_SUPPKEY] post-join-predicate [null])
                                                            SCAN (columns[34: PS_PARTKEY, 35: PS_SUPPKEY, 37: PS_SUPPLYCOST] predicate[null])
                                                            EXCHANGE BROADCAST
                                                                INNER JOIN (join-predicate [43: S_NATIONKEY = 48: N_NATIONKEY] post-join-predicate [null])
                                                                    SCAN (columns[40: S_SUPPKEY, 43: S_NATIONKEY] predicate[null])
                                                                    EXCHANGE BROADCAST
                                                                        INNER JOIN (join-predicate [50: N_REGIONKEY = 53: R_REGIONKEY] post-join-predicate [null])
                                                                            SCAN (columns[50: N_REGIONKEY, 48: N_NATIONKEY] predicate[null])
                                                                            EXCHANGE BROADCAST
                                                                                SCAN (columns[53: R_REGIONKEY, 54: R_NAME] predicate[54: R_NAME = AMERICA])
[end]

