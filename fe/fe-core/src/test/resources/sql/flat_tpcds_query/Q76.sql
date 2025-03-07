-- query 76
select  channel, col_name, d_year, d_qoy, i_category, COUNT(*) sales_cnt, SUM(ext_sales_price) sales_amt FROM (
        SELECT 'store' as channel, 'ss_store_sk' col_name,year(ss_sold_date) d_year, quarter(ss_sold_date) d_qoy, i_category, ss_ext_sales_price ext_sales_price
         FROM store_sales_flat, item_flat
         WHERE ss_store_sk IS NULL
           AND ss_item_sk=i_item_sk
        UNION ALL
        SELECT 'web' as channel, 'ws_ship_customer_sk' col_name,  year(ws_sold_date) d_year, quarter(ws_sold_date) d_qoy, i_category, ws_ext_sales_price ext_sales_price
         FROM web_sales_flat, item_flat
         WHERE ws_ship_customer_sk IS NULL
           AND ws_item_sk=i_item_sk
        UNION ALL
        SELECT 'catalog' as channel, 'cs_ship_addr_sk' col_name, year(cs_sold_date) d_year, quarter(cs_sold_date) d_qoy, i_category, cs_ext_sales_price ext_sales_price
         FROM catalog_sales_flat, item_flat
         WHERE cs_ship_addr_sk IS NULL
           AND cs_item_sk=i_item_sk) foo
GROUP BY channel, col_name, d_year, d_qoy, i_category
ORDER BY channel, col_name, d_year, d_qoy, i_category
limit 100;
