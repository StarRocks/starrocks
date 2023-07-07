[sql]
SELECT
  SUM(lo_extendedprice * lo_discount) AS revenue
FROM 
  lineorder,
  dates
WHERE 
  lo_orderdate = d_datekey
  AND d_yearmonthnum = 199401
  AND lo_discount BETWEEN 4 AND 6
  AND lo_quantity BETWEEN 26 AND 35;
[result]
AGGREGATE ([GLOBAL] aggregate [{36: sum=sum(36: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{36: sum=sum(multiply(cast(46: LO_EXTENDEDPRICE as bigint(20)), cast(48: LO_DISCOUNT as bigint(20))))}] group by [[]] having [null]
            SCAN (mv[lineorder_flat_mv] columns[45: LO_QUANTITY, 46: LO_EXTENDEDPRICE, 48: LO_DISCOUNT, 79: d_yearmonthnum] predicate[48: LO_DISCOUNT >= 4 AND 48: LO_DISCOUNT <= 6 AND 79: d_yearmonthnum = 199401 AND 45: LO_QUANTITY <= 35 AND 45: LO_QUANTITY >= 26])
[end]

