-- query 23
with frequent_ss_items as 
 (select substr(i_item_desc,1,30) itemdesc,i_item_sk item_sk, ss_sold_date solddate,count(*) cnt
  from store_sales_flat
      ,item_flat
  where ss_item_sk = i_item_sk 
    and ss_sold_date between '2000-01-01' and '2003-12-31'
  group by substr(i_item_desc,1,30),i_item_sk,ss_sold_date
  having count(*) >4),
 max_store_sales as
 (select max(csales) tpcds_cmax 
  from (select c_customer_sk,sum(ss_quantity*ss_sales_price) csales
        from store_sales_flat
            ,customer_flat
        where ss_customer_sk = c_customer_sk
         and ss_sold_date between '2000-01-01' and '2003-12-31' 
        group by c_customer_sk) t1),
 best_ss_customer as
 (select c_customer_sk,sum(ss_quantity*ss_sales_price) ssales
  from store_sales_flat
      ,customer_flat
  where ss_customer_sk = c_customer_sk
  group by c_customer_sk
  having sum(ss_quantity*ss_sales_price) > (50/100.0) * (select
  *
from
 max_store_sales))
  select  sum(sales)
 from (select cs_quantity*cs_list_price sales
       from catalog_sales_flat
       where cs_sold_date between '2000-02-01' and '2000-02-29'
         and cs_item_sk in (select item_sk from frequent_ss_items)
         and cs_bill_customer_sk in (select c_customer_sk from best_ss_customer)
      union all
      select ws_quantity*ws_list_price sales
       from web_sales_flat
       where ws_sold_date between '2000-02-01' and '2000-02-29'
         and ws_item_sk in (select item_sk from frequent_ss_items)
         and ws_bill_customer_sk in (select c_customer_sk from best_ss_customer)) t2
 limit 100;
