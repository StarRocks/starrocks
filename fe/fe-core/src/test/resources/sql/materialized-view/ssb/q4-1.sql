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
TOP-N (order by [[5: d_year ASC NULLS FIRST, 22: c_nation ASC NULLS FIRST]])
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 22: c_nation ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 22: c_nation]] having [null]
            EXCHANGE SHUFFLE[5, 22]
                AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 22: c_nation]] having [null]
                    SCAN (columns[73: LO_REVENUE, 74: LO_SUPPLYCOST, 81: C_NATION, 82: C_REGION, 89: S_REGION, 92: P_MFGR, 102: d_year] predicate[82: C_REGION = AMERICA AND 89: S_REGION = AMERICA AND 92: P_MFGR IN (MFGR#1, MFGR#2)])
[end]
