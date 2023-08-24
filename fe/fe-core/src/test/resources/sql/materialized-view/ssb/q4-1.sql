[sql]
SELECT
  d_year,
  c_nation,
  SUM(lo_revenue - lo_supplycost) AS profit
FROM 
  dates,
  customer,
  supplier,
  part,
  lineorder
WHERE 
  lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND lo_partkey = p_partkey
  AND lo_orderdate = d_datekey
  AND c_region = 'AMERICA'
  AND s_region = 'AMERICA'
  AND (p_mfgr = 'MFGR#1'
    OR p_mfgr = 'MFGR#2')
GROUP BY 
  d_year,
  c_nation
ORDER BY 
  d_year,
  c_nation;
[result]
Decode
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 115: C_NATION ASC NULLS FIRST]])
        TOP-N (order by [[5: d_year ASC NULLS FIRST, 115: C_NATION ASC NULLS FIRST]])
            AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 115: C_NATION]] having [null]
                EXCHANGE SHUFFLE[5, 115]
                    AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 115: C_NATION]] having [null]
                        SCAN (mv[lineorder_flat_mv] columns[115: C_NATION, 116: C_REGION, 117: S_REGION, 102: d_year, 118: P_MFGR, 73: LO_REVENUE, 74: LO_SUPPLYCOST] predicate[DictMapping(116: C_REGION{82: C_REGION = AMERICA}) AND DictMapping(117: S_REGION{89: S_REGION = AMERICA}) AND DictMapping(118: P_MFGR{92: P_MFGR IN (MFGR#1, MFGR#2)})])
[end]

