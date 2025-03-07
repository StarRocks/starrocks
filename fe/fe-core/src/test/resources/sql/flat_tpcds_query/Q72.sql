-- query 72
select  i_item_desc
      ,w_warehouse_name
      ,week(cs_sold_date) d_week_seq
      ,sum(case when p_promo_sk is null then 1 else 0 end) no_promo
      ,sum(case when p_promo_sk is not null then 1 else 0 end) promo
      ,count(*) total_cnt
from catalog_sales_flat cs
join inventory_flat on (cs_item_sk = inv_item_sk)
join item_flat on (i_item_sk = cs_item_sk)
left outer join catalog_returns_flat on (cr_item_sk = cs_item_sk and cr_order_number = cs_order_number)
where date_trunc('week', cs_sold_date) =  date_trunc('week', inv_date)
  and inv_quantity_on_hand < cs_quantity 
  and cs_ship_date > days_add(cs_sold_date, 5)
  and cs.bill_hd_buy_potential = '>10000'
  and year(cs_sold_date) = 1999
  and cs.bill_cd_marital_status = 'D'
group by i_item_desc,w_warehouse_name,cs_sold_date
order by total_cnt desc, i_item_desc, w_warehouse_name, d_week_seq
limit 100;

