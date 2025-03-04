-- DDL for store_returns_flat Table
CREATE TABLE
  store_returns_flat (
    -- Store Returns Columns
    sr_item_sk BIGINT,
    sr_customer_sk BIGINT,
    sr_cdemo_sk BIGINT,
    sr_hdemo_sk BIGINT,
    sr_addr_sk BIGINT,
    sr_store_sk BIGINT,
    sr_reason_sk BIGINT,
    sr_ticket_number BIGINT,
    sr_return_quantity INT,
    sr_return_amt DECIMAL(7, 2),
    sr_return_tax DECIMAL(7, 2),
    sr_return_amt_inc_tax DECIMAL(7, 2),
    sr_fee DECIMAL(7, 2),
    sr_return_ship_cost DECIMAL(7, 2),
    sr_refunded_cash DECIMAL(7, 2),
    sr_reversed_charge DECIMAL(7, 2),
    sr_store_credit DECIMAL(7, 2),
    sr_net_loss DECIMAL(7, 2),

    -- Customer Address Columns
    ca_address_sk BIGINT, 
    ca_street_number VARCHAR(255), 
    ca_street_name VARCHAR(255), 
    ca_street_type VARCHAR(255), 
    ca_suite_number VARCHAR(255), 
    ca_city VARCHAR(255), 
    ca_county VARCHAR(255), 
    ca_state VARCHAR(255), 
    ca_zip VARCHAR(255), 
    ca_country VARCHAR(255), 
    ca_gmt_offset DECIMAL(5,2), 
    ca_location_type VARCHAR(255), 

    -- Customer Demographics Columns
    cd_demo_sk BIGINT, 
    cd_gender VARCHAR(10), 
    cd_marital_status VARCHAR(10), 
    cd_education_status VARCHAR(255), 
    cd_purchase_estimate INT, 
    cd_credit_rating VARCHAR(50), 
    cd_dep_count INT, 
    cd_dep_employed_count INT, 
    cd_dep_college_count INT, 

    -- Household Demographics Columns
    hd_demo_sk BIGINT, 
    hd_income_band_sk BIGINT, 
    hd_buy_potential VARCHAR(50), 
    hd_dep_count INT, 
    hd_vehicle_count INT, 

    -- Income Band Columns
    ib_income_band_sk BIGINT, 
    ib_lower_bound INT, 
    ib_upper_bound INT,
    -- Store Columns
    s_store_sk BIGINT,
    s_store_id VARCHAR(255),
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk BIGINT,
    s_store_name VARCHAR(255),
    s_number_employees INT,
    s_floor_space INT,
    s_hours VARCHAR(255),
    s_manager VARCHAR(255),
    s_market_id INT,
    s_geography_class VARCHAR(255),
    s_market_desc VARCHAR(255),
    s_market_manager VARCHAR(255),
    s_division_id INT,
    s_division_name VARCHAR(255),
    s_company_id INT,
    s_company_name VARCHAR(255),
    s_street_number VARCHAR(255),
    s_street_name VARCHAR(255),
    s_street_type VARCHAR(255),
    s_suite_number VARCHAR(255),
    s_city VARCHAR(255),
    s_county VARCHAR(255),
    s_state VARCHAR(255),
    s_zip VARCHAR(255),
    s_country VARCHAR(255),
    s_gmt_offset DECIMAL(5, 2),
    s_tax_percentage DECIMAL(5, 2),
    -- Reason Columns
    r_reason_sk BIGINT,
    r_reason_id VARCHAR(255),
    r_reason_desc VARCHAR(255),
    -- Constructed Returned Datetime Column (Using DATETIME Type)
    returned_datetime DATETIME,
    sr_returned_date DATE
  ) PARTITION BY(sr_returned_date)
PROPERTIES (
  "replication_num" = "1"
);
