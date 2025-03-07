-- query 65
select 
	sb.s_store_name s_store_name,
	i_item_desc,
	sc.revenue,
	i_current_price,
	i_wholesale_cost,
	i_brand
 from item_flat,
     (select ss_store_sk,s_store_name, avg(revenue) as ave
 	from
 	    (select  ss_store_sk, s_store_name, ss_item_sk, 
 		     sum(ss_sales_price) as revenue
 		from store_sales_flat
 		where ss_sold_date between '1999-01-01' and '1999-12-31'
 		group by ss_store_sk,s_store_name, ss_item_sk) sa
 	group by ss_store_sk,s_store_name) sb,
     (select  ss_store_sk, s_store_name, ss_item_sk, sum(ss_sales_price) as revenue
 	from store_sales_flat
 	where ss_sold_date between '1999-01-01' and '1999-12-31'
 	group by ss_store_sk, s_store_name, ss_item_sk) sc
 where sb.ss_store_sk = sc.ss_store_sk and 
       sc.revenue <= 0.1 * sb.ave and
       i_item_sk = sc.ss_item_sk
order by s_store_name, i_item_desc
limit 100;
