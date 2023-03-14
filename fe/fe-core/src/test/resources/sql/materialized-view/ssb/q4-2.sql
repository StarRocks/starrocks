[sql]
SELECT
  d_year,
  s_nation,
  p_category,
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
  AND (d_year = 1997
    OR d_year = 1998)
  AND (p_mfgr = 'MFGR#1'
    OR p_mfgr = 'MFGR#2')
GROUP BY 
  d_year,
  s_nation,
  p_category
ORDER BY 
  d_year,
  s_nation,
  p_category;
[result]
TOP-N (order by [[5: d_year ASC NULLS FIRST, 30: s_nation ASC NULLS FIRST, 36: p_category ASC NULLS FIRST]])
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 30: s_nation ASC NULLS FIRST, 36: p_category ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 30: s_nation, 36: p_category]] having [null]
            EXCHANGE SHUFFLE[5, 30, 36]
                AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 30: s_nation, 36: p_category]] having [null]
                    SCAN (columns[73: LO_REVENUE, 74: LO_SUPPLYCOST, 82: C_REGION, 88: S_NATION, 89: S_REGION, 92: P_MFGR, 93: P_CATEGORY, 102: d_year] predicate[82: C_REGION = AMERICA AND 89: S_REGION = AMERICA AND 102: d_year IN (1997, 1998) AND 92: P_MFGR IN (MFGR#1, MFGR#2)])
[end]