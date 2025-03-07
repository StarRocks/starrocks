-- query 88
select  *
from
 (select count(*) h8_30_to_9
 from store_sales_flat
 where hour(sold_datetime) = 8
     and minute(sold_datetime) >= 30
     and ((hd_dep_count = 4 and hd_vehicle_count<=4+2) or
          (hd_dep_count = 2 and hd_vehicle_count<=2+2) or
          (hd_dep_count = 0 and hd_vehicle_count<=0+2)) 
     and s_store_name = 'ese') s1,
 (select count(*) h9_to_9_30 
 from store_sales_flat
 where 
     hour(sold_datetime) = 9
     and minute(sold_datetime) <= 30
     and ((hd_dep_count = 4 and hd_vehicle_count<=4+2) or
          (hd_dep_count = 2 and hd_vehicle_count<=2+2) or
          (hd_dep_count = 0 and hd_vehicle_count<=0+2))
     and s_store_name = 'ese') s2;
