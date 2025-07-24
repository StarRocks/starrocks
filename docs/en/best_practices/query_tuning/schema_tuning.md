---
displayed_sidebar: docs
sidebar_position: 50
---

# Schema Tuning Best Practices

This document provides comprehensive guidance for optimizing query performance in StarRocks through effective schema design and table configuration. By understanding how different table types, keys, distribution strategies, and indexing options impact query execution, you can significantly improve both speed and resource efficiency.

## Table of Contents

- [Table Type Selection](#table-type-selection)
- [Distribution and Partitioning](#distribution-and-partitioning)
- [Indexing Strategies](#indexing-strategies)
- [Colocate Tables](#colocate-tables)
- [Schema Design Patterns](#schema-design-patterns)
- [Schema Changes](#schema-changes)
- [Performance Optimization Tips](#performance-optimization-tips)
- [Related Resources](#related-resources)

## Table Type Selection

StarRocks supports four table types, each optimized for different use cases. All tables are sorted by their KEY columns for efficient querying.

### Aggregate Key Tables

**Use Case**: Pre-aggregated analytics, OLAP scenarios, dimensional analysis

Aggregate Key tables automatically merge records with identical keys using specified aggregation functions. This is ideal for metrics and fact tables where you want to pre-compute aggregations.

**Supported Aggregation Functions**:
- `SUM`: Numerical summation
- `MIN`/`MAX`: Minimum/maximum values
- `REPLACE`: Latest value overwrites (similar to UNIQUE KEY)
- `REPLACE_IF_NOT_NULL`: Replace only if new value is not null
- `HLL_UNION`: HyperLogLog union for approximate distinct counting
- `BITMAP_UNION`: Bitmap union for exact distinct counting

```sql
-- Example: Website analytics table
CREATE TABLE site_analytics (
    date_key        DATE,
    site_id         INT,
    city_id         SMALLINT,
    page_views      BIGINT SUM DEFAULT '0',
    unique_visitors BIGINT SUM DEFAULT '0',
    bounce_rate     DECIMAL(5,4) REPLACE DEFAULT '0.0000',
    last_updated    DATETIME REPLACE
)
AGGREGATE KEY(date_key, site_id, city_id)
DISTRIBUTED BY HASH(site_id)
PROPERTIES(
    "replication_num" = "3"
);
```

### Duplicate Key Tables

**Use Case**: Raw data storage, event logging, time-series data

Duplicate Key tables store all records without aggregation, making them perfect for detailed analysis and data exploration.

```sql
-- Example: User session tracking
CREATE TABLE user_sessions (
    user_id         BIGINT,
    session_id      VARCHAR(64),
    event_time      DATETIME,
    page_url        VARCHAR(1024),
    user_agent      VARCHAR(512),
    referrer        VARCHAR(1024),
    ip_address      VARCHAR(45),
    country_code    CHAR(2),
    device_type     VARCHAR(32)
)
DUPLICATE KEY(user_id, session_id, event_time)
DISTRIBUTED BY HASH(user_id)
PROPERTIES(
    "replication_num" = "3"
);
```

### Unique Key Tables

**Use Case**: Dimension tables, slowly changing dimensions, upsert scenarios

Unique Key tables ensure only one record exists per key, with new records overwriting existing ones.

```sql
-- Example: Customer dimension table
CREATE TABLE customers (
    customer_id     BIGINT,
    email           VARCHAR(255),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    registration_date DATE,
    status          VARCHAR(20),
    last_login      DATETIME,
    total_orders    INT DEFAULT '0'
)
UNIQUE KEY(customer_id)
DISTRIBUTED BY HASH(customer_id)
PROPERTIES(
    "replication_num" = "3"
);
```

### Primary Key Tables

**Use Case**: Real-time updates, transactional workloads, CDC scenarios

Primary Key tables provide ACID guarantees and support real-time updates with better performance than Unique Key tables for frequent updates.

For comprehensive guidance on Primary Key table design, performance optimization, and best practices, see **[Primary Key Table Best Practices](../primarykey_table.md)**.

```sql
-- Example: Real-time order tracking
CREATE TABLE orders_realtime (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_status    VARCHAR(20),
    order_amount    DECIMAL(10,2),
    created_at      DATETIME,
    updated_at      DATETIME
)
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id)
PROPERTIES(
    "replication_num" = "3",
    "enable_persistent_index" = "true"
);
```

## Distribution and Partitioning

Proper partitioning and bucketing strategies are fundamental to StarRocks performance. For comprehensive guidance on these topics, see our dedicated best practices guides:

- **[Partitioning Best Practices](../partitioning.md)** - Complete guide to time-based and composite partitioning strategies
- **[Bucketing Best Practices](../bucketing.md)** - In-depth comparison of Hash vs Random bucketing with practical examples
- **[Table Clustering](../table_clustering.md)** - Advanced clustering strategies for improved query performance

### Partitioning Strategy

StarRocks supports **RANGE partitioning** for the first level, typically by date/time columns.

**Benefits of Time-based Partitioning**:
- Efficient data lifecycle management
- Improved query performance through partition pruning
- Support for tiered storage (hot/cold data)
- Faster data deletion by dropping partitions

```sql
-- Example: Partitioned fact table
CREATE TABLE sales_fact (
    transaction_date DATE,
    store_id        INT,
    product_id      INT,
    quantity        INT,
    revenue         DECIMAL(10,2)
)
DUPLICATE KEY(transaction_date, store_id, product_id)
PARTITION BY RANGE(transaction_date) (
    PARTITION p202301 VALUES [('2023-01-01'), ('2023-02-01')),
    PARTITION p202302 VALUES [('2023-02-01'), ('2023-03-01')),
    PARTITION p202303 VALUES [('2023-03-01'), ('2023-04-01'))
)
DISTRIBUTED BY HASH(store_id, product_id) BUCKETS 32;
```

For detailed partitioning strategies including multi-tenant scenarios, granularity selection, and mixed granularity approaches, see **[Partitioning Best Practices](../partitioning.md)**.

### Bucketing Strategy

**HASH bucketing** distributes data across nodes for parallel processing.

**Best Practices**:
- Choose high-cardinality columns for even distribution
- Avoid columns with severe data skew
- Target 100MB-1GB of compressed data per bucket
- Use multiple columns for better distribution if needed

```sql
-- Good: High cardinality distribution
DISTRIBUTED BY HASH(user_id) BUCKETS 32;

-- Better: Multiple columns for even distribution
DISTRIBUTED BY HASH(user_id, product_id) BUCKETS 32;

-- Avoid: Low cardinality causing skew
-- DISTRIBUTED BY HASH(country) BUCKETS 32;  -- Don't do this
```

For comprehensive bucketing guidance including Random bucketing, operational guidelines, and trade-offs, see **[Bucketing Best Practices](../bucketing.md)**.

## Indexing Strategies

### Sparse Index Optimization

StarRocks automatically creates sparse indexes (every 1024 rows) using the first 36 bytes of the sort key.

**Key Principles**:
1. Place most selective filter columns first
2. Place frequently queried columns early
3. Put VARCHAR columns at the end (they truncate the index)
4. Order by query frequency and selectivity

```sql
-- Optimized column order for sparse index
CREATE TABLE user_events (
    user_id         BIGINT,        -- High selectivity, frequent filter
    event_date      DATE,          -- Common filter, good for partitioning
    event_type      VARCHAR(32),   -- Moderate selectivity
    session_id      VARCHAR(64),   -- High selectivity but VARCHAR
    event_data      JSON           -- Low selectivity, analysis column
)
DUPLICATE KEY(user_id, event_date, event_type, session_id)
DISTRIBUTED BY HASH(user_id);
```

### Bloom Filter Index

Use Bloom Filter indexes for high-cardinality columns that need exact match filtering.

```sql
-- Create Bloom Filter index for VARCHAR columns
CREATE TABLE products (
    product_id      BIGINT,
    product_name    VARCHAR(255),
    category        VARCHAR(100),
    brand           VARCHAR(100),
    sku             VARCHAR(64)
)
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id)
PROPERTIES(
    "bloom_filter_columns" = "product_name,sku"
);
```

### Bitmap Index

Bitmap indexes are effective for low-cardinality columns (typically < 10,000 distinct values).

```sql
-- Create bitmap index for categorical columns
ALTER TABLE user_events ADD INDEX idx_event_type (event_type) USING BITMAP;
ALTER TABLE customers ADD INDEX idx_country (country_code) USING BITMAP;
ALTER TABLE products ADD INDEX idx_category (category) USING BITMAP;
```

### Inverted Index

Inverted indexes support full-text search and complex string operations.

```sql
-- Create inverted index for text search
CREATE TABLE documents (
    doc_id          BIGINT,
    title           VARCHAR(500),
    content         TEXT,
    tags            ARRAY<VARCHAR(50)>
)
DUPLICATE KEY(doc_id)
DISTRIBUTED BY HASH(doc_id)
PROPERTIES(
    "inverted_index_columns" = "title,content"
);
```

## Colocate Tables

Colocate tables share the same bucketing strategy, enabling local joins without network shuffling.

**Benefits**:
- Dramatically faster join performance
- Reduced network I/O
- Lower resource consumption

```sql
-- Create colocated tables for efficient joins
CREATE TABLE orders (
    order_id        BIGINT,
    customer_id     BIGINT,
    order_date      DATE,
    total_amount    DECIMAL(10,2)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(customer_id) BUCKETS 32
PROPERTIES(
    "colocate_with" = "customer_group"
);

CREATE TABLE order_items (
    order_id        BIGINT,
    item_id         BIGINT,
    quantity        INT,
    unit_price      DECIMAL(8,2)
)
DUPLICATE KEY(order_id, item_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 32
PROPERTIES(
    "colocate_with" = "customer_group"  -- Same group as orders table
);
```

## Schema Design Patterns

For advanced table organization and clustering strategies, see **[Table Clustering Best Practices](../table_clustering.md)**.

### Star Schema vs. Flat Tables

**Star Schema Advantages**:
- Flexible modeling and easier maintenance
- Efficient dimension updates
- Better storage utilization
- Supports multiple fact tables sharing dimensions

**Flat Table Use Cases**:
- Ultra-low latency requirements
- High query concurrency
- Simple aggregation patterns

```sql
-- Star Schema Example
-- Fact table
CREATE TABLE sales_fact (
    date_key        INT,
    customer_key    INT,
    product_key     INT,
    store_key       INT,
    quantity        INT,
    revenue         DECIMAL(10,2),
    cost            DECIMAL(10,2)
)
AGGREGATE KEY(date_key, customer_key, product_key, store_key)
DISTRIBUTED BY HASH(customer_key, product_key);

-- Dimension tables
CREATE TABLE dim_customer (
    customer_key    INT,
    customer_id     VARCHAR(50),
    customer_name   VARCHAR(200),
    segment         VARCHAR(50),
    region          VARCHAR(50)
)
UNIQUE KEY(customer_key)
DISTRIBUTED BY HASH(customer_key);
```

### Materialized Views (Rollups)

Create rollups to optimize specific query patterns or improve aggregation performance.

```sql
-- Base table with detailed data
CREATE TABLE web_analytics (
    timestamp       DATETIME,
    user_id         BIGINT,
    page_id         INT,
    session_id      VARCHAR(64),
    country         VARCHAR(50),
    device_type     VARCHAR(20),
    page_views      BIGINT SUM DEFAULT '1',
    time_spent      INT SUM DEFAULT '0'
)
AGGREGATE KEY(timestamp, user_id, page_id, session_id, country, device_type)
DISTRIBUTED BY HASH(user_id);

-- Rollup for country-level analysis
ALTER TABLE web_analytics 
ADD ROLLUP country_rollup (
    timestamp, 
    country, 
    device_type, 
    page_views, 
    time_spent
) DUPLICATE KEY(timestamp, country, device_type);

-- Rollup for hourly aggregation
ALTER TABLE web_analytics 
ADD ROLLUP hourly_rollup (
    timestamp, 
    country, 
    page_views, 
    time_spent
) AGGREGATE KEY(timestamp, country);
```

## Schema Changes

StarRocks supports three types of schema changes with different performance characteristics:

### Linked Schema Change (Fastest)
Operations that don't require data transformation:

```sql
-- Add new columns (always added at the end)
ALTER TABLE customers ADD COLUMN loyalty_points INT DEFAULT '0';
ALTER TABLE customers ADD COLUMN preferences JSON;
```

### Direct Schema Change (Medium)
Operations requiring data transformation but not reordering:

```sql
-- Modify column types (compatible changes)
ALTER TABLE customers MODIFY COLUMN phone VARCHAR(20);
ALTER TABLE products MODIFY COLUMN price DECIMAL(12,2);
```

### Sorted Schema Change (Slowest)
Operations requiring data reordering:

```sql
-- Drop columns from sort key
ALTER TABLE sales_fact DROP COLUMN old_dimension;

-- Change sort key order (requires table recreation)
-- This is typically avoided in production
```

## Performance Optimization Tips

### 1. Column Ordering Strategy

```sql
-- Optimal column ordering
CREATE TABLE optimized_table (
    -- 1. Primary filters (highest selectivity)
    user_id         BIGINT,
    date_partition  DATE,
    
    -- 2. Secondary filters
    category_id     INT,
    status          TINYINT,
    
    -- 3. Join keys
    product_id      BIGINT,
    
    -- 4. Metrics/aggregatable columns
    quantity        INT,
    revenue         DECIMAL(10,2),
    
    -- 5. VARCHAR columns (at the end)
    description     VARCHAR(1000),
    notes           TEXT
)
DUPLICATE KEY(user_id, date_partition, category_id, status)
DISTRIBUTED BY HASH(user_id);
```

### 2. Bucket Sizing Guidelines

```sql
-- Calculate optimal bucket count
-- Target: 100MB - 1GB compressed data per bucket
-- Formula: (Total_Data_Size_GB / Target_Size_Per_Bucket_GB)

-- For a 100GB table targeting 500MB per bucket:
DISTRIBUTED BY HASH(key_column) BUCKETS 200;
```

### 3. Partition Management

```sql
-- Dynamic partition for automated partition management
CREATE TABLE time_series_data (
    event_time      DATETIME,
    metric_name     VARCHAR(100),
    metric_value    DOUBLE
)
DUPLICATE KEY(event_time, metric_name)
PARTITION BY RANGE(event_time) ()
DISTRIBUTED BY HASH(metric_name)
PROPERTIES(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",  -- Keep 30 days of history
    "dynamic_partition.end" = "3",      -- Pre-create 3 days ahead
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "32"
);
```

### 4. Monitoring and Maintenance

```sql
-- Check table statistics
SHOW TABLE STATUS LIKE 'your_table_name';

-- Analyze partition sizes
SHOW PARTITIONS FROM your_table_name;

-- Check index usage
SHOW INDEX FROM your_table_name;

-- Optimize table after major changes
OPTIMIZE TABLE your_table_name;
```

## Related Resources

### Core Table Design Documentation
- **[Table Overview](../../table_design/StarRocks_table_design.md)** - Comprehensive guide to StarRocks table structure and design principles
- **[Table Types](../../table_design/table_types/table_types.md)** - Detailed comparison of Duplicate, Aggregate, Unique, and Primary Key tables
- **[Table Capabilities](../../table_design/table_types/table_capabilities.md)** - Feature comparison across different table types

### Distribution and Partitioning Best Practices
- **[Partitioning Best Practices](../partitioning.md)** - In-depth guide to time-based and composite partitioning strategies
- **[Bucketing Best Practices](../bucketing.md)** - Comprehensive comparison of Hash vs Random bucketing with practical examples
- **[Data Distribution](../../table_design/data_distribution/Data_distribution.md)** - Technical details on how data is distributed across nodes

### Indexing and Performance
- **[Indexes Overview](../../table_design/indexes/indexes.md)** - Complete guide to all index types in StarRocks
- **[Prefix Index and Sort Key](../../table_design/indexes/Prefix_index_sort_key.md)** - Detailed explanation of sparse indexes and sort key optimization
- **[Bloom Filter Index](../../table_design/indexes/Bloomfilter_index.md)** - Guide to creating and using Bloom Filter indexes
- **[Bitmap Index](../../table_design/indexes/Bitmap_index.md)** - Best practices for low-cardinality column indexing
- **[Inverted Index](../../table_design/indexes/inverted_index.md)** - Full-text search and string operation optimization

### Join Optimization
- **[Colocate Join](../../using_starrocks/Colocate_join.md)** - Complete guide to setting up and managing colocate joins for high-performance local joins

### Query Tuning Integration
- **[Query Tuning Introduction](./query_plan_intro.md)** - Overview of the complete query optimization process
- **[Query Profile Overview](./query_profile_overview.md)** - How to analyze query execution and identify bottlenecks
- **[Query Tuning Recipes](./query_profile_tuning_recipes.md)** - Symptom-driven diagnosis and performance optimization
- **[Query Hints](./query_hint.md)** - Using optimizer hints to guide query execution

### Specialized Table Types
- **[Primary Key Tables](../primarykey_table.md)** - Best practices for real-time update scenarios
- **[Table Clustering](../table_clustering.md)** - Advanced clustering strategies for improved query performance

### Operational Considerations
- **[Data Compression](../../table_design/data_compression.md)** - Optimizing storage and I/O performance
- **[Hybrid Tables](../../table_design/hybrid_table.md)** - Combining different storage strategies

## Conclusion

Effective schema design in StarRocks requires understanding your query patterns, data characteristics, and performance requirements. Start with these best practices:

1. **Choose the right table type** based on your use case
2. **Design optimal sort keys** with selective columns first
3. **Partition by time** for lifecycle management
4. **Distribute evenly** using high-cardinality columns
5. **Use appropriate indexes** for your query patterns
6. **Consider colocate tables** for frequent joins
7. **Monitor and adjust** based on actual performance

Regular monitoring and iterative optimization will help you achieve optimal performance for your specific workload.
