[sql]
SELECT
    SUM(lo_extendedprice * lo_discount) AS revenue
FROM
    lineorder,
    dates
WHERE
        lo_orderdate = d_datekey
  AND d_year = 1993
  AND lo_discount BETWEEN 1 AND 3
  AND lo_quantity < 25;
[result]
AGGREGATE ([GLOBAL] aggregate [{36: sum=sum(36: sum)}] group by [[]] having [null]
    EXCHANGE GATHER
        AGGREGATE ([LOCAL] aggregate [{36: sum=sum(multiply(cast(46: LO_EXTENDEDPRICE as bigint(20)), cast(48: LO_DISCOUNT as bigint(20))))}] group by [[]] having [null]
            SCAN (mv[lineorder_flat_mv] columns[45: LO_QUANTITY, 46: LO_EXTENDEDPRICE, 48: LO_DISCOUNT, 78: d_year] predicate[48: LO_DISCOUNT >= 1 AND 48: LO_DISCOUNT <= 3 AND 78: d_year = 1993 AND 45: LO_QUANTITY <= 24])
[end]

