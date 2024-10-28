columns=(lo_orderkey      
 lo_linenumber    
 lo_custkey       
 lo_partkey       
 lo_suppkey       
 lo_orderdate     
 lo_orderpriority 
 lo_shippriority  
 lo_quantity      
 lo_extendedprice 
 lo_ordtotalprice 
 lo_discount      
 lo_revenue       
 lo_supplycost    
 lo_tax           
 lo_commitdate    
 lo_shipmode
 )



for col in ${columns[@]}; do
    sql="select histogram($col, 100, 1.0) from (select * from lineorder order by $col limit 1000000) t; " 
    echo "$sql"
    mysql -h127.1 -P8033 -uroot ssb -vvv -e "$sql" | grep sec
done

exit 0;

select histogram(lo_orderkey, 100, 1.0) from (select * from lineorder order by lo_orderkey limit 1000000) t; 
1 row in set (0.37 sec)
select histogram(lo_linenumber, 100, 1.0) from (select * from lineorder order by lo_linenumber limit 1000000) t; 
1 row in set (0.26 sec)
select histogram(lo_custkey, 100, 1.0) from (select * from lineorder order by lo_custkey limit 1000000) t; 
1 row in set (0.57 sec)
select histogram(lo_partkey, 100, 1.0) from (select * from lineorder order by lo_partkey limit 1000000) t; 
1 row in set (0.63 sec)
select histogram(lo_suppkey, 100, 1.0) from (select * from lineorder order by lo_suppkey limit 1000000) t; 
1 row in set (0.56 sec)
select histogram(lo_orderdate, 100, 1.0) from (select * from lineorder order by lo_orderdate limit 1000000) t; 
1 row in set (0.27 sec)
select histogram(lo_orderpriority, 100, 1.0) from (select * from lineorder order by lo_orderpriority limit 1000000) t; 
1 row in set (0.38 sec)
select histogram(lo_shippriority, 100, 1.0) from (select * from lineorder order by lo_shippriority limit 1000000) t; 
1 row in set (0.10 sec)
select histogram(lo_quantity, 100, 1.0) from (select * from lineorder order by lo_quantity limit 1000000) t; 
1 row in set (0.47 sec)
select histogram(lo_extendedprice, 100, 1.0) from (select * from lineorder order by lo_extendedprice limit 1000000) t; 
1 row in set (0.66 sec)
select histogram(lo_ordtotalprice, 100, 1.0) from (select * from lineorder order by lo_ordtotalprice limit 1000000) t; 
1 row in set (0.53 sec)
select histogram(lo_discount, 100, 1.0) from (select * from lineorder order by lo_discount limit 1000000) t; 
1 row in set (0.45 sec)
select histogram(lo_revenue, 100, 1.0) from (select * from lineorder order by lo_revenue limit 1000000) t; 
1 row in set (0.67 sec)
select histogram(lo_supplycost, 100, 1.0) from (select * from lineorder order by lo_supplycost limit 1000000) t; 
1 row in set (0.60 sec)
select histogram(lo_tax, 100, 1.0) from (select * from lineorder order by lo_tax limit 1000000) t; 
1 row in set (0.40 sec)
select histogram(lo_commitdate, 100, 1.0) from (select * from lineorder order by lo_commitdate limit 1000000) t; 
1 row in set (0.49 sec)
select histogram(lo_shipmode, 100, 1.0) from (select * from lineorder order by lo_shipmode limit 1000000) t; 
1 row in set (0.46 sec)



-- optimization
select histogram(lo_orderkey, 100, 1.0) from (select * from lineorder order by lo_orderkey limit 1000000) t; 
1 row in set (0.39 sec)
select histogram(lo_linenumber, 100, 1.0) from (select * from lineorder order by lo_linenumber limit 1000000) t; 
1 row in set (0.23 sec)
select histogram(lo_custkey, 100, 1.0) from (select * from lineorder order by lo_custkey limit 1000000) t; 
1 row in set (0.55 sec)
select histogram(lo_partkey, 100, 1.0) from (select * from lineorder order by lo_partkey limit 1000000) t; 
1 row in set (0.65 sec)
select histogram(lo_suppkey, 100, 1.0) from (select * from lineorder order by lo_suppkey limit 1000000) t; 
1 row in set (0.51 sec)
select histogram(lo_orderdate, 100, 1.0) from (select * from lineorder order by lo_orderdate limit 1000000) t; 
1 row in set (0.28 sec)
select histogram(lo_orderpriority, 100, 1.0) from (select * from lineorder order by lo_orderpriority limit 1000000) t; 
1 row in set (0.40 sec)
select histogram(lo_shippriority, 100, 1.0) from (select * from lineorder order by lo_shippriority limit 1000000) t; 
1 row in set (0.09 sec)
select histogram(lo_quantity, 100, 1.0) from (select * from lineorder order by lo_quantity limit 1000000) t; 
1 row in set (0.48 sec)
select histogram(lo_extendedprice, 100, 1.0) from (select * from lineorder order by lo_extendedprice limit 1000000) t; 
1 row in set (0.66 sec)
select histogram(lo_ordtotalprice, 100, 1.0) from (select * from lineorder order by lo_ordtotalprice limit 1000000) t; 
1 row in set (0.53 sec)
select histogram(lo_discount, 100, 1.0) from (select * from lineorder order by lo_discount limit 1000000) t; 
1 row in set (0.44 sec)
select histogram(lo_revenue, 100, 1.0) from (select * from lineorder order by lo_revenue limit 1000000) t; 
1 row in set (0.69 sec)
select histogram(lo_supplycost, 100, 1.0) from (select * from lineorder order by lo_supplycost limit 1000000) t; 
1 row in set (0.62 sec)
select histogram(lo_tax, 100, 1.0) from (select * from lineorder order by lo_tax limit 1000000) t; 
1 row in set (0.41 sec)
select histogram(lo_commitdate, 100, 1.0) from (select * from lineorder order by lo_commitdate limit 1000000) t; 
1 row in set (0.60 sec)
select histogram(lo_shipmode, 100, 1.0) from (select * from lineorder order by lo_shipmode limit 1000000) t; 
1 row in set (0.49 sec)