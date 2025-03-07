-- query 24
with ssales as
(select c_last_name
      ,c_first_name
      ,ss.s_store_name
      ,ss.ca_state
      ,ss.s_state
      ,i_color
      ,i_current_price
      ,i_manager_id
      ,i_units
      ,i_size
      ,sum(ss_net_paid) netpaid
from store_sales_flat ss
    ,store_returns_flat
    ,item_flat
    ,customer_flat
where ss_ticket_number = sr_ticket_number
  and ss_item_sk = sr_item_sk
  and ss_customer_sk = c_customer_sk
  and ss_item_sk = i_item_sk
  and c_birth_country <> upper(ss.ca_country)
  and ss.s_zip = ss.ca_zip
and ss.s_market_id=8
group by c_last_name
        ,c_first_name
        ,ss.s_store_name
        ,ss.ca_state
        ,ss.s_state
        ,i_color
        ,i_current_price
        ,i_manager_id
        ,i_units
        ,i_size)
select c_last_name
      ,c_first_name
      ,s_store_name
      ,sum(netpaid) paid
from ssales
where i_color = 'peach'
group by c_last_name
        ,c_first_name
        ,s_store_name
having sum(netpaid) > (select 0.05*avg(netpaid)
                                 from ssales)
order by c_last_name
        ,c_first_name
        ,s_store_name
;
