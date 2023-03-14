[sql]
SELECT
  SUM(lo_revenue),
  d_year,
  p_brand
FROM 
  lineorder,
  dates,
  part,
  supplier
WHERE 
  lo_orderdate = d_datekey
  AND lo_partkey = p_partkey
  AND lo_suppkey = s_suppkey
  AND p_brand = 'MFGR#2221'
  AND s_region = 'EUROPE'
GROUP BY 
  d_year,
  p_brand
ORDER BY 
  d_year,
  p_brand;
[result]
Decode
    TOP-N (order by [[22: d_year ASC NULLS FIRST, 108: P_BRAND ASC NULLS FIRST]])
        TOP-N (order by [[22: d_year ASC NULLS FIRST, 108: P_BRAND ASC NULLS FIRST]])
            AGGREGATE ([GLOBAL] aggregate [{51: sum=sum(51: sum)}] group by [[22: d_year, 108: P_BRAND]] having [null]
                EXCHANGE SHUFFLE[22, 108]
                    AGGREGATE ([LOCAL] aggregate [{51: sum=sum(13: lo_revenue)}] group by [[22: d_year, 108: P_BRAND]] having [null]
                        SCAN (mv[lineorder_flat_mv] columns[107: S_REGION, 108: P_BRAND, 93: d_year, 64: LO_REVENUE] predicate[DictMapping(108: P_BRAND{85: P_BRAND = MFGR#2221}) AND DictMapping(107: S_REGION{80: S_REGION = EUROPE})])
[end]

