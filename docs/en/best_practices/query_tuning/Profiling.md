---
displayed_sidebar: docs
---

# Performance Optimization

## Table Type Selection

StarRocks supports four table types: Duplicate Key table, Aggregate table, Unique Key table, and Primary Key table. All of them are sorted by KEY.

- `AGGREGATE KEY`: When records with the same AGGREGATE KEY is loaded into StarRocks, the old and new records are aggregated. Currently, Aggregate tables supports the following aggregate functions: SUM, MIN, MAX, and REPLACE. Aggregate tables support aggregating data in advance, facilitating business statements and multi-dimensional analyses.
- `DUPLICATE KEY`: You only need to specify the sort key for a DUPLICATE KEY table. Records with the same DUPLICATE KEY exist at the same time. It is suitable for analyses that do not involve aggregating data in advance.
- `UNIQUE KEY`: When records with the same UNIQUE KEY is loaded into StarRocks, the new record overwrites the old one. A UNIQUE KEY tables is similar to an Aggregate table with REPLACE function. Both are suitable for analyses involving constant updates.
- `PRIMARY KEY`: Primary Key tables guarantee the uniqueness of records, and allow you to perform realtime updating.

~~~sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid);


CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid);

CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
PRIMARY KEY(orderid)
DISTRIBUTED BY HASH(orderid);
~~~

## Colocate Table

To speed up queries, tables with the same distribution can use a common bucketing column. In that case, data can be joined locally without being transferred across the cluster during the `join` operation.

~~~sql
CREATE TABLE colocate_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    browser      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid)
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

For more information about colocate join and replica management, see [Colocate join](../using_starrocks/Colocate_join.md)

## Flat table and star schema

StarRocks supports star schema, which is more flexible in modelling than flat tables. You can create a view to replace flat tables during modelling and then query data from multiple tables to accelerate queries.

Flat tables have the following drawbacks:

- Costly dimension updates because a flat table usually contains a massive number of dimensions. Each time a dimension is updated, the entire table must be updated. The situation exacerbates as the update frequency increases.
- High maintenance cost because flat tables require additional development workloads, storage space, and data backfilling operations.
- High data ingestion cost because a flat table has many fields and an Aggregate table may contain even more key fields. During data loading, more fields need to be sorted, which prolongs data loading.

If you have high requirements on query concurrency or low latency, you can still use flat tables.

## Partition and bucket

StarRocks supports two levels of partitioning: the first level is RANGE partition and the second level is HASH bucket.

- RANGE partition: RANGE partition is used to divide data into different intervals (can be understood as dividing the original table into multiple sub-tables). Most users choose to set partitions by time, which has the following advantages:

  - Easier to distinguish between hot and cold data
  - Be able to leverage StarRocks tiered storage (SSD + SATA)
  - Faster to delete data by partition

- HASH bucket: Divides data into different buckets according to the hash value.

  - It is recommended to use a column with a high degree of discrimination for bucketing to avoid data skew.
  - To facilitate data recovery, it is recommended to keep the size of compressed data in each bucket between 100 MB to 1 GB. We recommend you configure an appropriate number of buckets when you create a table or add a partition.
  - Random bucketing is not recommended. You must explicitly specify the HASH bucketing column when you create a table.

## Sparse index and bloomfilter index

StarRocks stores data in an ordered manner and builds sparse indexes at a granularity of 1024 rows.

StarRocks selects a fixed-length prefix (currently 36 bytes) in the schema as the sparse index.

When creating a table, it is recommended to place common filter fields at the beginning of the schema declaration. Fields with the highest differentiation and query frequency must be placed first.

A VARCHAR field must placed at the end of a sparse index because the index gets truncated from the VARCHAR field. If the VARCHAR field appears first, the index may be less than 36 bytes.

Use the above `site_visit` table as an example. The table has four columns: `siteid, city, username, pv`. The sort key contains three columns `siteid, city, username`, which occupy 4, 2, and 32 bytes respectively. So the prefix index (sparse index) can be the first 30 bytes of `siteid + city + username`.

In addition to sparse indexes, StarRocks also provides bloomfilter indexes, which are effective for filtering columns with high discrimination. If you want to place VARCHAR fields before other fields, you can create bloomfilter indexes.

## Inverted Index

StarRocks adopts Bitmap Indexing technology to support inverted indexes that can be applied to all columns of the Duplicate Key table and the key column of the Aggregate table and Unique Key table. Bitmap Index is suitable for columns with a small value range, such as gender, city, and province. As the range expands, the bitmap index expands in parallel.

## Materialized view (rollup)

A rollup is essentially a materialized index of the original table (base table). When creating a rollup, only some columns of the base table can be selected as the schema, and the order of the fields in the schema can be different from that of the base table. Below are some use cases of using a rollup:

- Data aggregation in the base table is not high, because the base table has fields with high differentiation. In this case, you may consider selecting some columns to create rollups. Use the above `site_visit` table as an example:

  ~~~sql
  site_visit(siteid, city, username, pv)
  ~~~

  `siteid` may lead to poor data aggregation. If you need to frequently calculate PVs by city, you can create a rollup with only `city` and `pv`.

  ~~~sql
  ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);
  ~~~

- The prefix index in the base table cannot be hit, because the way the base table is built cannot cover all the query patterns. In this case, you may consider creating a rollup to adjust the column order. Use the above `session_data` table as an example:

  ~~~sql
  session_data(visitorid, sessionid, visittime, city, province, ip, browser, url)
  ~~~

  If there are cases where you need to analyze visits by `browser` and `province` in addition to `visitorid`, you can create a separate rollup:

  ~~~sql
  ALTER TABLE session_data
  ADD ROLLUP rollup_browser(browser,province,ip,url)
  DUPLICATE KEY(browser,province);
  ~~~

## Schema change

There are three ways to change schemas in StarRocks: sorted schema change, direct schema change, and linked schema change.

- Sorted schema change: Change the sorting of a column and reorder the data. For example, deleting a column in a sorted schema leads to data reorder.

  `ALTER TABLE site_visit DROP COLUMN city;`

- Direct schema change: Transform the data instead of reordering it, for example, changing the column type or adding a column to a sparse index.

  `ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

- Linked schema change: Complete changes without transforming data, for example, adding columns.

  `ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

  It is recommended to choose an appropriate schema when you create tables to accelerate schema changes.
