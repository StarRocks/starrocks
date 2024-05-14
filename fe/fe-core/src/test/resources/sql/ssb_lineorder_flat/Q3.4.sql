SELECT /*+ SET_VAR(pipeline_dop=1) */ c_city, s_city, year(lo_orderdate) AS year, sum(lo_revenue) AS revenue
FROM lineorder_flat 
WHERE c_city not in ('UNITED KI1', 'UNITED KI5') AND s_city in ( 'UNITED KI1',  'UNITED KI5') AND  lo_orderdate  >= '1997-12-01' AND lo_orderdate <= '1997-12-31'
GROUP BY c_city,  s_city, year 
ORDER BY year ASC, revenue DESC; 
