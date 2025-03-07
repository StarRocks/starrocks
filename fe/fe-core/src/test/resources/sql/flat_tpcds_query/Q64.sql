-- query 64
with cs_ui as
 (select cs_item_sk
        ,sum(cs_ext_list_price) as sale,sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit) as refund
  from catalog_sales_flat
      ,catalog_returns_flat
  where cs_item_sk = cr_item_sk
    and cs_order_number = cr_order_number
  group by cs_item_sk
  having sum(cs_ext_list_price)>2*sum(cr_refunded_cash+cr_reversed_charge+cr_store_credit)),
cross_sales as
 (select 
       i_product_name product_name
       ,i_item_sk item_sk
       ,ss.s_store_name store_name
       ,ss.s_zip store_zip
       ,ss.ca_street_number b_street_number
       ,ss.ca_street_name b_street_name
       ,ss.ca_city b_city
       ,ss.ca_zip b_zip
       ,c.ca_street_number c_street_number
       ,c.ca_street_name c_street_name
       ,c.ca_city c_city
       ,c.ca_zip c_zip
       ,ss.ss_sold_date syear
       ,c_first_sales_date fsyear
       ,c_first_shipto_date s2year
     ,count(*) cnt
     ,sum(ss_wholesale_cost) s1
     ,sum(ss_list_price) s2
     ,sum(ss_coupon_amt) s3
  FROM   store_sales_flat ss
        ,store_returns_flat
        ,cs_ui
        ,customer_flat c
        ,item_flat
  WHERE  ss_customer_sk = c_customer_sk AND
         ss_item_sk = i_item_sk and
         ss_item_sk = sr_item_sk and
         ss_ticket_number = sr_ticket_number and
         ss_item_sk = cs_ui.cs_item_sk and
         ss.cd_marital_status <> c.cd_marital_status and
         i_color in ('purple','burlywood','indian','spring','floral','medium') and
         i_current_price between 64 and 64 + 10 and
         i_current_price between 64 + 1 and 64 + 15
group by i_product_name
       ,i_item_sk
       ,ss.s_store_name
       ,ss.s_zip
       ,ss.ca_street_number
       ,ss.ca_street_name
       ,ss.ca_city
       ,ss.ca_zip
       ,c.ca_street_number
       ,c.ca_street_name
       ,c.ca_city
       ,c.ca_zip
       ,ss.ss_sold_date
       ,c_first_sales_date
       ,c_first_shipto_date
)
select cs1.product_name
     ,cs1.store_name
     ,cs1.store_zip
     ,cs1.b_street_number
     ,cs1.b_street_name
     ,cs1.b_city
     ,cs1.b_zip
     ,cs1.c_street_number
     ,cs1.c_street_name
     ,cs1.c_city
     ,cs1.c_zip
     ,cs1.syear
     ,cs1.cnt
     ,cs1.s1 as s11
     ,cs1.s2 as s21
     ,cs1.s3 as s31
     ,cs2.s1 as s12
     ,cs2.s2 as s22
     ,cs2.s3 as s32
     ,cs2.syear
     ,cs2.cnt
from cross_sales cs1,cross_sales cs2
where cs1.item_sk=cs2.item_sk and
     year(cs1.syear) = 1999 and
     year(cs2.syear) = 1999 + 1 and
     cs2.cnt <= cs1.cnt and
     cs1.store_name = cs2.store_name and
     cs1.store_zip = cs2.store_zip
order by cs1.product_name
       ,cs1.store_name
       ,cs2.cnt
       ,cs1.s1
       ,cs2.s1;
