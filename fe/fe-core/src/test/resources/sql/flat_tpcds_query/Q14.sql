-- query 14
with  cross_items as
 (select i_item_sk ss_item_sk
 from item_flat,
 (select iss.i_brand_id brand_id
     ,iss.i_class_id class_id
     ,iss.i_category_id category_id
 from store_sales_flat
     ,item_flat iss
 where ss_item_sk = iss.i_item_sk
   and ss_sold_date between '1999-01-01' and years_add('1999-01-01',2)
 intersect 
 select ics.i_brand_id
     ,ics.i_class_id
     ,ics.i_category_id
 from catalog_sales_flat
     ,item_flat ics
 where cs_item_sk = ics.i_item_sk
   and cs_sold_date between '1999-01-01' and years_add('1999-01-01',2)
 intersect
 select iws.i_brand_id
     ,iws.i_class_id
     ,iws.i_category_id
 from web_sales_flat
     ,item_flat iws
 where ws_item_sk = iws.i_item_sk
   and ws_sold_date between '1999-01-01' and years_add('1999-01-01',2)) t
 where i_brand_id = brand_id
      and i_class_id = class_id
      and i_category_id = category_id
),
 avg_sales as
 (select avg(quantity*list_price) average_sales
  from (select ss_quantity quantity
             ,ss_list_price list_price
       from store_sales_flat
       where ss_sold_date between '1999-01-01' and years_add('1999-01-01',2)
       union all 
       select cs_quantity quantity 
             ,cs_list_price list_price
       from catalog_sales_flat
       where cs_sold_date between '1999-01-01' and years_add('1999-01-01',2) 
       union all
       select ws_quantity quantity
             ,ws_list_price list_price
       from web_sales_flat
       where ws_sold_date between '1999-01-01' and years_add('1999-01-01',2))  x)
select  channel, i_brand_id,i_class_id,i_category_id,sum(sales), sum(number_sales)
 from(
       select 'store' channel, i_brand_id,i_class_id
             ,i_category_id,sum(ss_quantity*ss_list_price) sales
             , count(*) number_sales
       from store_sales_flat
           ,item_flat
       where ss_item_sk in (select ss_item_sk from cross_items)
         and ss_item_sk = i_item_sk
         and ss_sold_date between '2001-11-01' and '2001-11-31'
       group by i_brand_id,i_class_id,i_category_id
       having sum(ss_quantity*ss_list_price) > (select average_sales from avg_sales)
       union all
       select 'catalog' channel, i_brand_id,i_class_id,i_category_id, sum(cs_quantity*cs_list_price) sales, count(*) number_sales
       from catalog_sales_flat
           ,item_flat
       where cs_item_sk in (select ss_item_sk from cross_items)
         and cs_item_sk = i_item_sk
         and cs_sold_date between '2001-11-01' and '2001-11-31'
       group by i_brand_id,i_class_id,i_category_id
       having sum(cs_quantity*cs_list_price) > (select average_sales from avg_sales)
       union all
       select 'web' channel, i_brand_id,i_class_id,i_category_id, sum(ws_quantity*ws_list_price) sales , count(*) number_sales
       from web_sales_flat
           ,item_flat
       where ws_item_sk in (select ss_item_sk from cross_items)
         and ws_item_sk = i_item_sk
         and ws_sold_date between '2001-11-01' and '2001-11-31'
       group by i_brand_id,i_class_id,i_category_id
       having sum(ws_quantity*ws_list_price) > (select average_sales from avg_sales)
 ) y
 group by rollup (channel, i_brand_id,i_class_id,i_category_id)
 order by channel,i_brand_id,i_class_id,i_category_id
 limit 100;