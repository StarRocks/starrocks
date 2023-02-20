# Performance Optimization

## Table Creation

### Data Model Selection

StarRocks supports three kinds of data model: AGGREGATE KEY, UNIQUE KEY, and DUPLICATE KEY. All three are sorted by KEY.

* AGGREGATE KEY: When the AGGREGATE KEY is the same, the old and new records are aggregated. The currently supported aggregate functions are SUM, MIN, MAX, REPLACE. Aggregate  model can aggregate data in advance, which is suitable for reporting and multi-dimensional analyses.

~~~sql
CREATE TABLE site_visit
(
    siteid      INT,
    city        SMALLINT,
    username    VARCHAR(32),
    pv BIGINT   SUM DEFAULT '0'
)
AGGREGATE KEY(siteid, city, username)
DISTRIBUTED BY HASH(siteid) BUCKETS 10;
~~~

UNIQUE KEY: When the UNIQUE KEY is the same, the new record overwrites the old one.. Currently, `UNIQUE KEY` functions similarly as `REPLACE` of `AGGREGATE KEY`.Both are suitable for analyses involving constant updates.

~~~sql
CREATE TABLE sales_order
(
    orderid     BIGINT,
    status      TINYINT,
    username    VARCHAR(32),
    amount      BIGINT DEFAULT '0'
)
UNIQUE KEY(orderid)
DISTRIBUTED BY HASH(orderid) BUCKETS 10;
~~~

DUPLICATE KEY: Only need to specify the sort key. Records with the same DUPLICATE KEY exist at the same time. It is suitable for analyses that don’t involve aggregating data in advance.

~~~sql
CREATE TABLE session_data
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10;
~~~

### In-memory tables

StarRocks supports caching data in memory to speed up queries. In-memory tables are suitable for dimension tables with a small number of rows.

~~~sql
CREATE TABLE memory_table
(
    visitorid   SMALLINT,
    sessionid   BIGINT,
    visittime   DATETIME,
    city        CHAR(20),
    province    CHAR(20),
    ip          varchar(32),
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
PROPERTIES (
           "in_memory"="true"
);
~~~

### Colocate Table

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
    brower      CHAR(20),
    url         VARCHAR(1024)
)
DUPLICATE KEY(visitorid, sessionid)
DISTRIBUTED BY HASH(sessionid, visitorid) BUCKETS 10
PROPERTIES(
    "colocate_with" = "group1"
);
~~~

For more information about colocate join and replica management, refer to [Colocate join](../using_starrocks/Colocate_join.md)

### Flat table and star schema

To adapt to the front-end business,  flat tables don’t  differentiate between dimension information and index information. Such flat tables often do not perform as well as expected because:

* The schema has many fields When there are a large number of key columns in the aggregation model,  it may lead to an increase in the number of columns that need to be sorted during the import.
* Updates on dimension information will be reflected to the table. The frequency of updates directly affects query efficiency.

It is recommended to use star schema to distinguish dimension tables and index tables. This schema can place dimension tables with frequent updates in MySQL, and dimension tables with fewer updates in StarRocks. Creating replicas of dimension tables in StarRocks can improve join performance.

### Partition and bucket

StarRocks supports two levels of partitioning storage, the first level is RANGE partition, the second level is HASH bucket.

* RANGE partition: RANGE partition is used to divide the data into different intervals (can be understood as dividing the original table into multiple sub-tables). Most users choose to set partitions by time, which has the following advantages:

* Data can be distinguished between hot and cold
* Be able to leverage StarRocks tiered storage (SSD + SATA)
* Faster to delete data by partition

* HASH bucket: Divides data into different buckets according to the hash value.

* It is recommended to use a column with a large degree of differentiation for bucketing to avoid data skewing
* To facilitate data recovery, it is recommended to keep  the size of each bucket around 10GB. Please consider the number of buckets when creating a table or adding a partition .

### Sparse index and bloomfilter

StarRocks stores data in an ordered manner and builds a sparse index with  block (1024 rows) granularity.

* The sparse index selects a fixed-length prefix (currently 36 bytes) in the schema as the index content.

* When creating a table, it is recommended to put the common filter fields ahead of the schema. The most differentiated and frequent the query fields should be placed first.

* A varchar field should be placed to the end of a  sparse index. If the varchar field appears first and gets truncated, the index may be less than 36 bytes long.

* For the above `site_visit` table

    `site_visit(siteid, city, username, pv)`

* The siteid, city, and username rows occupy 4, 2, and 32 bytes respectively, so the content of the prefix index is the first 30 bytes of siteid + city + username.
* In addition to sparse indexes, StarRocks also provides bloomfilter indexes, which are effective for filtering columns with large differentiations. If you don't want to place a varchar field in the sparse index, you can create a bloomfilter index.

### Inverted Index

StarRocks adopts Bitmap Indexing technology to support inverted indexes that can be applied to all columns of the duplicate data model and the key column of the aggregate and unique data models. Bitmap Index is suitable for columns with a small value range, such as gender, city, and province. As the range expands, the bitmap index expands in parallel.

### Materialized view (rollup)

A rollup is essentially a materialized index of the original table (base table). When creating a rollup, only some columns of the base table can be selected as the schema, and the order of the fields in the schema can be different from that of the base table. Below are some use cases of using a rollup:

* The data aggregation in the base table is not high, because the base table has differentiated fields. In this case, you may consider selecting some columns to create rollups. For the `site_visit` table above:

`site_visit(siteid, city, username, pv)`

* `siteid` may lead to poor data aggregation. You may create a rollup with only `city` and `pv`.

`ALTER TABLE site_visit ADD ROLLUP rollup_city(city, pv);`

* The prefix index in the base table cannot be hit, because the way the base table is built cannot cover all the query patterns. In this case, you may consider creating a rollup to adjust the column order. For the `session_data` table above:

`session_data(visitorid, sessionid, visittime, city, province, ip, brower, url)`

* If there are cases where you can analyze visits by browser and province, you can create a separate rollup.

`ALTER TABLE session_data ADD ROLLUP rollup_brower(brower,province,ip,url) DUPLICATE KEY(brower,province);`

## Import

StarRocks currently supports two types of imports – broker load and stream load. StarRocks guarantees atomicity for single batch imports, even if multiple tables are imported at once.

* stream load: Import by http in micro batch. Latency of importingf 1 MB data is maintained at the second level. Suitable for high frequency import.
* broker load: Import by pull. Suitable for batch data import on a daily basis.

## Schema change

There are three ways to change schemas in StarRocks –  sorted schema change, direct schema change, and linked schema change.

* Sorted schema change: Change the sorting of a column and reorder the data. For example, deleting a column in a sorted schema leads to data reorder.

`ALTER TABLE site_visit DROP COLUMN city;`

* Direct schema change: Transform the data instead of reordering it.For example, changing the column type, adding a column to a sparse index, etc.

`ALTER TABLE site_visit MODIFY COLUMN username varchar(64);`

* Linked schema change: Complete changes without transforming the  data. For example, adding columns.

`ALTER TABLE site_visit ADD COLUMN click bigint SUM default '0';`

It is recommended to consider schemas during table creation to avoid problems in schema changes.
