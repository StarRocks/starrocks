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
Decode
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 116: S_NATION ASC NULLS FIRST, 119: P_CATEGORY ASC NULLS FIRST]])
        TOP-N (order by [[5: d_year ASC NULLS FIRST, 116: S_NATION ASC NULLS FIRST, 119: P_CATEGORY ASC NULLS FIRST]])
            AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 116: S_NATION, 119: P_CATEGORY]] having [null]
                EXCHANGE SHUFFLE[5, 116, 119]
                    AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 116: S_NATION, 119: P_CATEGORY]] having [null]
                        SCAN (mv[lineorder_flat_mv] columns[115: C_REGION, 116: S_NATION, 117: S_REGION, 102: d_year, 118: P_MFGR, 119: P_CATEGORY, 73: LO_REVENUE, 74: LO_SUPPLYCOST] predicate[DictMapping(115: C_REGION, 82: C_REGION = AMERICA) AND DictMapping(117: S_REGION, 89: S_REGION = AMERICA) AND DictMapping(118: P_MFGR, 92: P_MFGR IN (MFGR#1, MFGR#2)) AND 102: d_year IN (1997, 1998)])
[end]

