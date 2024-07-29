SELECT /*+ SET_VAR(pipeline_dop=1) */ sum(lo_extendedprice * lo_discount) AS `revenue`
FROM lineorder_flat 
WHERE lo_orderdate >= '1993-01-01' and lo_orderdate <= '1993-12-31' AND lo_discount BETWEEN 1 AND 3 AND lo_quantity < 25;
