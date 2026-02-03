-- Production-like table models for StarRocks
-- Covers: PRIMARY KEY, UNIQUE KEY, AGGREGATE KEY, DUPLICATE KEY
-- Note: StarRocks does not enforce foreign keys; relations are logical.

CREATE DATABASE IF NOT EXISTS prod_models_demo;
USE prod_models_demo;

-- ========= Dimension tables (slow-changing, upsert-friendly) =========
CREATE TABLE IF NOT EXISTS dim_user (
    user_id BIGINT NOT NULL,
    user_name VARCHAR(128),
    phone VARCHAR(32),
    email VARCHAR(128),
    city_id INT,
    register_time DATETIME,
    status TINYINT,
    update_time DATETIME
)
ENGINE=OLAP
PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGINT NOT NULL,
    sku_id BIGINT,
    product_name VARCHAR(256),
    category_id INT,
    brand_id INT,
    list_price DECIMAL(18, 2),
    is_active BOOLEAN,
    update_time DATETIME
)
ENGINE=OLAP
UNIQUE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3"
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGINT NOT NULL,
    store_name VARCHAR(128),
    region VARCHAR(64),
    city_id INT,
    open_date DATE,
    status TINYINT,
    update_time DATETIME
)
ENGINE=OLAP
UNIQUE KEY(store_id)
DISTRIBUTED BY HASH(store_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "3"
);

-- ========= Fact tables (append-heavy, analytics oriented) =========
-- Relation: fact_order.user_id -> dim_user.user_id
-- Relation: fact_order.store_id -> dim_store.store_id
CREATE TABLE IF NOT EXISTS fact_order (
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    order_time DATETIME,
    user_id BIGINT,
    store_id BIGINT,
    pay_method TINYINT,
    order_status TINYINT,
    total_amount DECIMAL(18, 2),
    total_discount DECIMAL(18, 2),
    update_time DATETIME
)
ENGINE=OLAP
DUPLICATE KEY(order_id, order_date)
PARTITION BY RANGE(order_date) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01"),
    PARTITION p202403 VALUES LESS THAN ("2024-04-01"),
    PARTITION pmax VALUES LESS THAN ("9999-12-31")
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3"
);

-- Relation: fact_order_item.order_id -> fact_order.order_id
-- Relation: fact_order_item.product_id -> dim_product.product_id
CREATE TABLE IF NOT EXISTS fact_order_item (
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    item_id BIGINT NOT NULL,
    product_id BIGINT,
    quantity INT,
    item_price DECIMAL(18, 2),
    item_discount DECIMAL(18, 2),
    update_time DATETIME
)
ENGINE=OLAP
DUPLICATE KEY(order_id, order_date, item_id)
PARTITION BY RANGE(order_date) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01"),
    PARTITION p202403 VALUES LESS THAN ("2024-04-01"),
    PARTITION pmax VALUES LESS THAN ("9999-12-31")
)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3"
);

-- ========= Aggregated model (pre-aggregation) =========
-- Daily sales summary for reports.
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    sales_date DATE NOT NULL,
    store_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    order_cnt BIGINT SUM DEFAULT "0",
    item_cnt BIGINT SUM DEFAULT "0",
    gross_amount DECIMAL(18, 2) SUM DEFAULT "0",
    discount_amount DECIMAL(18, 2) SUM DEFAULT "0",
    pay_amount DECIMAL(18, 2) SUM DEFAULT "0"
)
ENGINE=OLAP
AGGREGATE KEY(sales_date, store_id, product_id)
PARTITION BY RANGE(sales_date) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01"),
    PARTITION p202403 VALUES LESS THAN ("2024-04-01"),
    PARTITION pmax VALUES LESS THAN ("9999-12-31")
)
DISTRIBUTED BY HASH(store_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3"
);

-- ========= Event/log model (high volume, append only) =========
-- Relation: user_activity_log.user_id -> dim_user.user_id
CREATE TABLE IF NOT EXISTS user_activity_log (
    event_time DATETIME NOT NULL,
    user_id BIGINT,
    session_id VARCHAR(64),
    page_id VARCHAR(128),
    event_type VARCHAR(32),
    event_properties JSON,
    device_type VARCHAR(32),
    ip_address VARCHAR(64)
)
ENGINE=OLAP
DUPLICATE KEY(event_time, user_id, session_id)
PARTITION BY RANGE(event_time) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01 00:00:00"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01 00:00:00"),
    PARTITION p202403 VALUES LESS THAN ("2024-04-01 00:00:00"),
    PARTITION pmax VALUES LESS THAN ("9999-12-31 00:00:00")
)
DISTRIBUTED BY HASH(user_id) BUCKETS 32
PROPERTIES (
    "replication_num" = "3"
);

-- ========= Inventory model (upsert latest status) =========
-- Relation: inventory_snapshot.product_id -> dim_product.product_id
-- Relation: inventory_snapshot.store_id -> dim_store.store_id
CREATE TABLE IF NOT EXISTS inventory_snapshot (
    store_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    snapshot_date DATE NOT NULL,
    on_hand_qty INT,
    reserved_qty INT,
    available_qty INT,
    update_time DATETIME
)
ENGINE=OLAP
PRIMARY KEY(store_id, product_id, snapshot_date)
PARTITION BY RANGE(snapshot_date) (
    PARTITION p202401 VALUES LESS THAN ("2024-02-01"),
    PARTITION p202402 VALUES LESS THAN ("2024-03-01"),
    PARTITION p202403 VALUES LESS THAN ("2024-04-01"),
    PARTITION pmax VALUES LESS THAN ("9999-12-31")
)
DISTRIBUTED BY HASH(store_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
