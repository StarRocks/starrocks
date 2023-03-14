[sql]
SELECT
  d_year,
  s_city,
  p_brand,
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
  AND s_nation = 'UNITED STATES'
  AND (d_year = 1997
    OR d_year = 1998)
  AND p_category = 'MFGR#14'
GROUP BY 
  d_year,
  s_city,
  p_brand
ORDER BY 
  d_year,
  s_city,
  p_brand;
[result]
<<<<<<< HEAD
TOP-N (order by [[5: d_year ASC NULLS FIRST, 29: s_city ASC NULLS FIRST, 37: p_brand ASC NULLS FIRST]])
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 29: s_city ASC NULLS FIRST, 37: p_brand ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 29: s_city, 37: p_brand]] having [null]
            EXCHANGE SHUFFLE[5, 29, 37]
                AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 29: s_city, 37: p_brand]] having [null]
                    SCAN (columns[73: LO_REVENUE, 74: LO_SUPPLYCOST, 82: C_REGION, 87: S_CITY, 88: S_NATION, 93: P_CATEGORY, 94: P_BRAND, 102: d_year] predicate[93: P_CATEGORY = MFGR#14 AND 82: C_REGION = AMERICA AND 88: S_NATION = UNITED STATES AND 102: d_year IN (1997, 1998)])
=======
Decode
    TOP-N (order by [[5: d_year ASC NULLS FIRST, 116: S_CITY ASC NULLS FIRST, 119: P_BRAND ASC NULLS FIRST]])
        TOP-N (order by [[5: d_year ASC NULLS FIRST, 116: S_CITY ASC NULLS FIRST, 119: P_BRAND ASC NULLS FIRST]])
            AGGREGATE ([GLOBAL] aggregate [{60: sum=sum(60: sum)}] group by [[5: d_year, 116: S_CITY, 119: P_BRAND]] having [null]
                EXCHANGE SHUFFLE[5, 116, 119]
                    AGGREGATE ([LOCAL] aggregate [{60: sum=sum(59: expr)}] group by [[5: d_year, 116: S_CITY, 119: P_BRAND]] having [null]
                        SCAN (mv[lineorder_flat_mv] columns[115: C_REGION, 116: S_CITY, 117: S_NATION, 102: d_year, 118: P_CATEGORY, 119: P_BRAND, 73: LO_REVENUE, 74: LO_SUPPLYCOST] predicate[DictMapping(115: C_REGION{82: C_REGION = AMERICA}) AND DictMapping(117: S_NATION{88: S_NATION = UNITED STATES}) AND DictMapping(118: P_CATEGORY{93: P_CATEGORY = MFGR#14}) AND 102: d_year IN (1997, 1998)])
>>>>>>> f241c36fa ([Enhancement] Support TPCH Benchmark for MV (#18506))
[end]

