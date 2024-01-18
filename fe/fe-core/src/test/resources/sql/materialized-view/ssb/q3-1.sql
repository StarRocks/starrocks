[sql]
SELECT
  c_nation,
  s_nation,
  d_year,
  SUM(lo_revenue) AS revenue
FROM 
  customer,
  lineorder,
  supplier,
  dates
WHERE 
  lo_custkey = c_custkey
  AND lo_suppkey = s_suppkey
  AND lo_orderdate = d_datekey
  AND c_region = 'ASIA'
  AND s_region = 'ASIA'
  AND d_year >= 1992
  AND d_year <= 1997
GROUP BY 
  c_nation,
  s_nation,
  d_year
ORDER BY 
  d_year ASC, 
  revenue DESC;
[result]
TOP-N (order by [[37: d_year ASC NULLS FIRST, 50: sum DESC NULLS LAST]])
    TOP-N (order by [[37: d_year ASC NULLS FIRST, 50: sum DESC NULLS LAST]])
        Decode
            AGGREGATE ([GLOBAL] aggregate [{50: sum=sum(50: sum)}] group by [[106: C_NATION, 108: S_NATION, 37: d_year]] having [null]
                EXCHANGE SHUFFLE[106, 108, 37]
                    AGGREGATE ([LOCAL] aggregate [{50: sum=sum(21: lo_revenue)}] group by [[106: C_NATION, 108: S_NATION, 37: d_year]] having [null]
<<<<<<< HEAD
                        SCAN (mv[lineorder_flat_mv] columns[106: C_NATION, 107: C_REGION, 92: d_year, 108: S_NATION, 109: S_REGION, 63: LO_REVENUE] predicate[DictMapping(107: C_REGION{72: C_REGION = ASIA}) AND DictMapping(109: S_REGION{79: S_REGION = ASIA}) AND 92: d_year >= 1992 AND 92: d_year <= 1997])
=======
                        SCAN (mv[lineorder_flat_mv] columns[106: C_NATION, 107: C_REGION, 92: d_year, 108: S_NATION, 109: S_REGION, 63: LO_REVENUE] predicate[DictMapping(109: S_REGION{79: S_REGION = ASIA}) AND 92: d_year >= 1992 AND 92: d_year <= 1997 AND DictMapping(107: C_REGION{72: C_REGION = ASIA})])
>>>>>>> 2.5.18
[end]

