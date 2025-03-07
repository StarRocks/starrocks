-- query 3
select  year(ss_sold_date) as d_year
       ,item.i_brand_id brand_id 
       ,item.i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  store_sales_flat store_sales
      ,item_flat item
 where store_sales.ss_item_sk = item.i_item_sk
   and item.i_manufact_id = 128
   and month(ss_sold_date) =11
 group by 1,2,3
 order by d_year
         ,sum_agg desc
         ,brand_id
 limit 100;