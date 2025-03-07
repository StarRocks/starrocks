-- query 78
with ws as
  (select year(ws_sold_date) AS ws_sold_year, ws_item_sk,
    ws_bill_customer_sk ws_customer_sk,
    sum(ws_quantity) ws_qty,
    sum(ws_wholesale_cost) ws_wc,
    sum(ws_sales_price) ws_sp
   from web_sales_flat
   left join web_returns_flat on wr_order_number=ws_order_number and ws_item_sk=wr_item_sk
   where wr_order_number is null
   group by ws_sold_year, ws_item_sk, ws_bill_customer_sk
   ),
cs as
  (select year(cs_sold_date) AS cs_sold_year, cs_item_sk,
    cs_bill_customer_sk cs_customer_sk,
    sum(cs_quantity) cs_qty,
    sum(cs_wholesale_cost) cs_wc,
    sum(cs_sales_price) cs_sp
   from catalog_sales_flat
   left join catalog_returns_flat on cr_order_number=cs_order_number and cs_item_sk=cr_item_sk
   where cr_order_number is null
   group by cs_sold_year, cs_item_sk, cs_bill_customer_sk
   ),
ss as
  (select year(ss_sold_date) AS ss_sold_year, ss_item_sk,
    ss_customer_sk,
    sum(ss_quantity) ss_qty,
    sum(ss_wholesale_cost) ss_wc,
    sum(ss_sales_price) ss_sp
   from store_sales_flat
   left join store_returns_flat on sr_ticket_number=ss_ticket_number and ss_item_sk=sr_item_sk
   where sr_ticket_number is null
   group by ss_sold_year, ss_item_sk, ss_customer_sk
   )
 select 
ss_sold_year, ss_item_sk, ss_customer_sk,
round(ss_qty/(coalesce(ws_qty,0)+coalesce(cs_qty,0)),2) ratio,
ss_qty store_qty, ss_wc store_wholesale_cost, ss_sp store_sales_price,
coalesce(ws_qty,0)+coalesce(cs_qty,0) other_chan_qty,
coalesce(ws_wc,0)+coalesce(cs_wc,0) other_chan_wholesale_cost,
coalesce(ws_sp,0)+coalesce(cs_sp,0) other_chan_sales_price
from ss
left join ws on (ws_sold_year=ss_sold_year and ws_item_sk=ss_item_sk and ws_customer_sk=ss_customer_sk)
left join cs on (cs_sold_year=ss_sold_year and cs_item_sk=ss_item_sk and cs_customer_sk=ss_customer_sk)
where (coalesce(ws_qty,0)>0 or coalesce(cs_qty, 0)>0) and ss_sold_year=2000
order by 
  ss_sold_year, ss_item_sk, ss_customer_sk,
  ss_qty desc, ss_wc desc, ss_sp desc,
  other_chan_qty,
  other_chan_wholesale_cost,
  other_chan_sales_price,
  ratio
limit 100;

