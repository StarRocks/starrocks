-- query 67
select  *
from (select i_category
            ,i_class
            ,i_brand
            ,i_product_name
            ,d_year
            ,d_qoy
            ,d_moy
            ,s_store_id
            ,sumsales
            ,rank() over (partition by i_category order by sumsales desc) rk
      from (select i_category
                  ,i_class
                  ,i_brand
                  ,i_product_name
                  ,year(ss_sold_date) d_year
                  ,quarter(ss_sold_date) d_qoy
                  ,month(ss_sold_date) d_moy
                  ,s_store_id
                  ,sum(coalesce(ss_sales_price*ss_quantity,0)) sumsales
            from store_sales_flat
                ,item_flat
       where ss_item_sk=i_item_sk
          and ss_sold_date between '2000-01-01' and '2000-12-31'
       group by  rollup(i_category, i_class, i_brand, i_product_name, d_year, d_qoy, d_moy,s_store_id))dw1) dw2
where rk <= 100
order by i_category
        ,i_class
        ,i_brand
        ,i_product_name
        ,d_year
        ,d_qoy
        ,d_moy
        ,s_store_id
        ,sumsales
        ,rk
limit 100;

