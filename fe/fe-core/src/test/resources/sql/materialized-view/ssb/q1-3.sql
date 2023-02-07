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
[end]
