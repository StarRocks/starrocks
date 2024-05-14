SELECT /*+ SET_VAR(pipeline_dop=1) */ sum(lo_extendedprice * lo_discount) AS revenue
FROM lineorder_flat 
WHERE weekofyear(lo_orderdate) = 6 AND lo_orderdate >= '1994-01-01' and lo_orderdate <= '1994-12-31' 
 AND lo_discount BETWEEN 5 AND 7 AND lo_quantity BETWEEN 26 AND 35;
