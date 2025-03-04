-- DDL for inventory_flat Table
CREATE TABLE inventory_flat (
    -- Inventory Columns
    inv_date_sk BIGINT, 
    inv_item_sk BIGINT, 
    inv_warehouse_sk BIGINT, 
    inv_quantity_on_hand INT, 

    -- Warehouse Columns
    w_warehouse_sk BIGINT, 
    w_warehouse_id VARCHAR(255), 
    w_warehouse_name VARCHAR(255), 
    w_warehouse_sq_ft INT, 
    w_street_number VARCHAR(255), 
    w_street_name VARCHAR(255), 
    w_street_type VARCHAR(255), 
    w_suite_number VARCHAR(255), 
    w_city VARCHAR(255), 
    w_county VARCHAR(255), 
    w_state VARCHAR(255), 
    w_zip VARCHAR(255), 
    w_country VARCHAR(255), 
    w_gmt_offset DECIMAL(5,2),
    inv_date DATE
) PARTITION BY(inv_date)

PROPERTIES (
  "replication_num" = "1"
);
