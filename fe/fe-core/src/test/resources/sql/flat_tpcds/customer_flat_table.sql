-- DDL for customer_flat Table
CREATE TABLE customer_flat (
    -- Customer Columns
    c_customer_sk BIGINT, 
    c_customer_id VARCHAR(255), 
    c_current_cdemo_sk BIGINT, 
    c_current_hdemo_sk BIGINT, 
    c_current_addr_sk BIGINT, 
    c_first_shipto_date DATE, 
    c_salutation VARCHAR(255), 
    c_first_name VARCHAR(255), 
    c_last_name VARCHAR(255), 
    c_preferred_cust_flag VARCHAR(255), 
    c_birth_day INT, 
    c_birth_month INT, 
    c_birth_year INT, 
    c_birth_country VARCHAR(255), 
    c_login VARCHAR(255), 
    c_email_address VARCHAR(255), 
    c_last_review_date BIGINT, 

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
    c_first_sales_date DATE 
) PARTITION BY(c_first_sales_date)

PROPERTIES (
  "replication_num" = "1"
);
