[sql]
SELECT
  SUM(lo_extendedprice * lo_discount) AS revenue
FROM 
  lineorder,
  dates
WHERE 
  lo_orderdate = d_datekey
  AND d_weeknuminyear = 6
  AND d_year = 1994
  AND lo_discount BETWEEN 5 AND 7
  AND lo_quantity BETWEEN 26 AND 35;
[result]
AGGREGATE ([GLOBAL] aggregate [{36: sum=sum(36: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{36: sum=sum(multiply(cast(46: LO_EXTENDEDPRICE as bigint(20)), cast(48: LO_DISCOUNT as bigint(20))))}] group by [[]] having [null]
            SCAN (mv[lineorder_flat_mv] columns[45: LO_QUANTITY, 46: LO_EXTENDEDPRICE, 48: LO_DISCOUNT, 78: d_year, 85: d_weeknuminyear] predicate[48: LO_DISCOUNT >= 5 AND 48: LO_DISCOUNT <= 7 AND 78: d_year = 1994 AND 85: d_weeknuminyear = 6 AND 45: LO_QUANTITY >= 26 AND 45: LO_QUANTITY <= 35])
[end]

