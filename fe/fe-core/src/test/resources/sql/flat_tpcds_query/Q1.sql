with
  customer_total_return as (
    select
      s_state,
      sr_customer_sk as ctr_customer_sk,
      sr_store_sk as ctr_store_sk,
      sum(SR_RETURN_AMT) as ctr_total_return
    from
      store_returns_flat
    where
      year(sr_returned_date) = 2000
    group by
      s_state,
      sr_customer_sk,
      sr_store_sk
  )
select
  c_customer_id
from
  customer_total_return ctr1,
  customer_flat
where
  ctr1.ctr_total_return > (
    select
      avg(ctr_total_return) * 1.2
    from
      customer_total_return ctr2
    where
      ctr1.ctr_store_sk = ctr2.ctr_store_sk
  )
  and ctr1.s_state = 'TN'
  and ctr1.ctr_customer_sk = c_customer_sk
order by
  c_customer_id
limit
  100;