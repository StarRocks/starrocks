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
        AGGREGATE ([GLOBAL] aggregate [{50: sum=sum(50: sum)}] group by [[5: c_nation, 30: s_nation, 37: d_year]] having [null]
            EXCHANGE SHUFFLE[5, 30, 37]
                AGGREGATE ([LOCAL] aggregate [{50: sum=sum(21: lo_revenue)}] group by [[5: c_nation, 30: s_nation, 37: d_year]] having [null]
                    SCAN (columns[63: LO_REVENUE, 71: C_NATION, 72: C_REGION, 78: S_NATION, 79: S_REGION, 92: d_year] predicate[92: d_year <= 1997 AND 92: d_year >= 1992 AND 72: C_REGION = ASIA AND 79: S_REGION = ASIA])
[end]
