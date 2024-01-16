---
displayed_sidebar: "English"
---

# Data lake query acceleration with materialized views

This topic describes how to optimize query performance in your data lake using StarRocks' asynchronous materialized views.

StarRocks offers out-of-the-box data lake query capabilities, which are highly effective for exploratory queries and analysis of data in the lake. In most scenarios, [Data Cache](../data_source/data_cache.md) can provide block-level file caching, avoiding performance degradation caused by remote storage jitter and a large number of I/O operations.

However, when it comes to building complex and efficient reports using data from the lake or further accelerating these queries, you may still encounter performance challenges. With asynchronous materialized views, you can achieve higher concurrency and better query performance for reports and data applications on the lake.

## Overview

StarRocks supports building asynchronous materialized views based on external catalogs such as Hive catalog, Iceberg catalog, and Hudi catalog. External catalog-based materialized views are particularly useful in the following scenarios:

- **Transparent Acceleration of data lake reports**

  To ensure the query performance of data lake reports, data engineers typically need to work closely with data analysts to probe into the construction logic of the acceleration layer for reports. If the acceleration layer requires further updates, they must update the construction logic, processing schedules, and query statements accordingly.

  Through the query rewrite capability of materialized views, report acceleration can be made transparent and imperceptible to users. When slow queries are identified, data engineers can analyze the pattern of slow queries and create materialized views on demand. Application-side queries are then intelligently rewritten and transparently accelerated by the materialized view, allowing for rapid improvement in query performance without modifying the logic of the business application or the query statement.

- **Incremental calculation of real-time  data associated with historical data**

  Suppose your business application requires the association of real-time data in StarRocks native tables and historical data in the data lake for incremental calculations. In this situation, materialized views can provide a straightforward solution. For example, if the real-time fact table is a native table in StarRocks and the dimension table is stored in the data lake, you can easily perform incremental calculations by constructing materialized views that associate the native table with the table in the external data sources.

- **Rapid construction of metric layers**

  Calculating and processing metrics may encounter challenges when dealing with a high dimensionality of data. You can use materialized views, which allows you to perform data pre-aggregation and rollup, to create a relatively lightweight metric layer. Moreover, materialized views can be refreshed automatically, further reducing the complexity of metric calculations.

Materialized views, Data Cache, and native tables in StarRocks are all effective methods to achieve significant boosts in query performance. The following table compares their major differences:

<table class="comparison">
  <thead>
    <tr>
      <th>&nbsp;</th>
      <th>Data Cache</th>
      <th>Materialized view</th>
      <th>Native table</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>Data loading and updates</b></td>
      <td>Queries automatically trigger data caching.</td>
      <td>Refresh tasks are triggered automatically.</td>
      <td>Supports various import methods but requires manual maintenance of import tasks</td>
    </tr>
    <tr>
      <td><b>Data caching granularity</b></td>
      <td><ul><li>Supports block-level data caching</li><li>Follows LRU cache eviction mechanism</li><li>No computation results are cached</li></ul></td>
      <td>Stores the precomputed query results</td>
      <td>Stores data based on table schema</td>
    </tr>
    <tr>
      <td><b>Query performance</b></td>
      <td colspan="3" style={{textAlign: 'center'}} >Data Cache &le; Materialized view = Native table</td>
    </tr>
    <tr>
      <td><b>Query statement</b></td>
      <td><ul><li>No need to modify query statements against the data lake</li><li>Once queries hit the cache, computation occurs.</li></ul></td>
      <td><ul><li>No need to modify query statements against the data lake</li><li>Leverages query rewrite to reuse precomputed results</li></ul></td>
      <td>Requires modification of query statements to query the native table</td>
    </tr>
  </tbody>
</table>

<br />

Compared to directly querying lake data or loading data into native tables, materialized views offer several unique advantages:

- **Local storage  acceleration**: Materialized views can leverage StarRocks' acceleration advantages with local storage, such as indexes, partitioning, bucketing, and collocate groups, resulting in better query performance compared to querying data from the data lake directly.
- **Zero maintenance for loading tasks**: Materialized views update data transparently via automatic refresh tasks. There's no need to maintain loading tasks to perform scheduled data updates. Additionally, Hive catalog-based materialized views can detect data changes and perform incremental refreshes at the partition level.
- **Intelligent  query  rewrite**: Queries can be transparently rewritten to use materialized views. You can benefit from acceleration instantly without the need to modify the query statements your application uses.

<br />

Therefore, we recommend using materialized views in the following scenarios:

- Even when Data Cache is enabled, query performance does not meet your requirements for query latency and concurrency.
- Queries involve reusable components, such as fixed aggregation functions or join patterns.
- Data is organized in partitions, while queries involve aggregation on a relatively high level (e.g., aggregating by day).

<br />

In the following scenarios, we recommend prioritizing acceleration through Data Cache:

- Queries do not have many reusable components and may scan any data from the data lake.
- Remote storage has significant fluctuations or instability, which could potentially impact access.

## Create external catalog-based materialized views

Creating a materialized view on tables in external catalogs is similar to creating a materialized view on StarRiocks' native tables. You only need to set a suitable refresh strategy in accordance with the data source you are using, and manually enable query rewrite for external catalog-based materialized views.

### Choose a suitable refresh strategy

Currently, StarRocks cannot detect partition-level data changes in Hudi catalogs, Iceberg catalogs, and JDBC catalogs. Therefore, a full-size refresh is performed once the task is triggered.

For Hive catalogs, you can enable the Hive metadata cache refresh feature to allow StarRocks to detect data changes at the partition level. Please note that the partitioning keys of the materialized view must be included in that of the base table. When this feature is enabled, StarRocks periodically accesses the Hive Metastore Service (HMS) or AWS Glue to check the metadata information of recently queried hot data. As a result, StarRocks can:

- Refresh only the partitions with data changes to avoid full-size refresh, reducing resource consumption caused by refresh.

- Ensure data consistency to some extent during query rewrite. If there are data changes in the base table in the data lake, the query will not be rewritten to use the materialized view.

  > **NOTE**
  >
  > You can still choose to tolerate a certain level of data inconsistency by setting the property `mv_rewrite_staleness_second` when creating the materialized view. For more information, see [CREATE MATERIALIZED VIEW](../sql-reference/sql-statements/data-definition/CREATE_MATERIALIZED_VIEW.md).

To enable the Hive metadata cache refresh feature, you can set the following FE dynamic configuration item using [ADMIN SET FRONTEND CONFIG](../sql-reference/sql-statements/Administration/ADMIN_SET_CONFIG.md):

| **Configuration item**                                       | **Default**                | **Description**                                              |
| ------------------------------------------------------------ | -------------------------- | ------------------------------------------------------------ |
| enable_background_refresh_connector_metadata                 | true in v3.0 false in v2.5 | Whether to enable the periodic Hive metadata cache refresh. After it is enabled, StarRocks polls the metastore (Hive Metastore or AWS Glue) of your Hive cluster, and refreshes the cached metadata of the frequently accessed Hive catalogs to perceive data changes. true indicates to enable the Hive metadata cache refresh, and false indicates to disable it. |
| background_refresh_metadata_interval_millis                  | 600000 (10 minutes)        | The interval between two consecutive Hive metadata cache refreshes. Unit: millisecond. |
| background_refresh_metadata_time_secs_since_last_access_secs | 86400 (24 hours)           | The expiration time of a Hive metadata cache refresh task. For the Hive catalog that has been accessed, if it has not been accessed for more than the specified time, StarRocks stops refreshing its cached metadata. For the Hive catalog that has not been accessed, StarRocks will not refresh its cached metadata. Unit: second. |

### Enable query rewrite for external catalog-based materialized views

By default, StarRocks does not support query rewrite for materialized views built on Hudi, Iceberg, and JDBC catalogs because query rewrite in this scenario cannot ensure a strong consistency of results. You can enable this feature by setting the property `force_external_table_query_rewrite` to `true` when creating the materialized view. For materialized views built on tables in Hive catalogs, the query rewrite is enabled by default.

Example:

```SQL
CREATE MATERIALIZED VIEW ex_mv_par_tbl
PARTITION BY emp_date
DISTRIBUTED BY hash(empid)
PROPERTIES (
"force_external_table_query_rewrite" = "true"
) 
AS
select empid, deptno, emp_date
from `hive_catalog`.`emp_db`.`emps_par_tbl`
where empid < 5;
```

In scenarios involving query rewriting, if you use a very complex query statement to build a materialized view, we recommend that you split the query statement and construct multiple simple materialized views in a nested manner. Nested materialized views are more versatile and can accommodate a broader range of query patterns.

## Best practices

In real-world business scenarios, you can identify queries with high execution latency and resource consumption by analyzing audit logs. You can further use [query profiles](../administration/query_profile.md) to pinpoint the specific stages where the query is slow. The following sections provide instructions and examples on how to boost data lake query performance with materialized views.

### Case One: Accelerate join calculation in data lake

You can use materialized views to accelerate join queries in the data lake.

Suppose the following queries on your Hive catalog are particularly slow:

```SQL
--Q1
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_year = 1993
    AND lo_discount BETWEEN 1 AND 3
    AND lo_quantity < 25;

--Q2
SELECT SUM(lo_extendedprice * lo_discount) AS REVENUE
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
WHERE
    lo_orderdate = d_datekey
    AND d_yearmonth = 'Jan1994'
    AND lo_discount BETWEEN 4 AND 6
    AND lo_quantity BETWEEN 26 AND 35;

--Q3 
SELECT SUM(lo_revenue), d_year, p_brand
FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates, hive.ssb_1g_csv.part, hive.ssb_1g_csv.supplier
WHERE
    lo_orderdate = d_datekey
    AND lo_partkey = p_partkey
    AND lo_suppkey = s_suppkey
    AND p_brand BETWEEN 'MFGR#2221' AND 'MFGR#2228'
    AND s_region = 'ASIA'
GROUP BY d_year, p_brand
ORDER BY d_year, p_brand;
```

By analyzing their query profiles, you may notice that the query execution time is mostly spent on the hash join between the table `lineorder` and the other dimension tables on the column `lo_orderdate`.

Here, Q1 and Q2 perform aggregation after joining `lineorder` and `dates`, while Q3 performs aggregation after joining `lineorder`, `dates`, `part`, and `supplier`.

Therefore, you can utilize the [View Delta Join rewrite](./query_rewrite_with_materialized_views.md#view-delta-join-rewrite) capability of StarRocks to build a materialized view that joins `lineorder`, `dates`, `part`, and `supplier`.

```SQL
CREATE MATERIALIZED VIEW lineorder_flat_mv
DISTRIBUTED BY HASH(LO_ORDERDATE, LO_ORDERKEY) BUCKETS 48
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES ( 
    -- Specify the unique constraints.
    "unique_constraints" = "
    hive.ssb_1g_csv.supplier.s_suppkey;
    hive.ssb_1g_csv.part.p_partkey;
    hive.ssb_1g_csv.dates.d_datekey",
    -- Specify the Foreign Keys.
    "foreign_key_constraints" = "
    hive.ssb_1g_csv.lineorder(lo_partkey) REFERENCES hive.ssb_1g_csv.part(p_partkey);
    hive.ssb_1g_csv.lineorder(lo_suppkey) REFERENCES hive.ssb_1g_csv.supplier(s_suppkey);
    hive.ssb_1g_csv.lineorder(lo_orderdate) REFERENCES hive.ssb_1g_csv.dates(d_datekey)",
    -- Enable query rewrite for the external catalog-based materialized view.
    "force_external_table_query_rewrite" = "TRUE"
)
AS SELECT
       l.LO_ORDERDATE AS LO_ORDERDATE,
       l.LO_ORDERKEY AS LO_ORDERKEY,
       l.LO_PARTKEY AS LO_PARTKEY,
       l.LO_SUPPKEY AS LO_SUPPKEY,
       l.LO_QUANTITY AS LO_QUANTITY,
       l.LO_EXTENDEDPRICE AS LO_EXTENDEDPRICE,
       l.LO_DISCOUNT AS LO_DISCOUNT,
       l.LO_REVENUE AS LO_REVENUE,
       s.S_REGION AS S_REGION,
       p.P_BRAND AS P_BRAND,
       d.D_YEAR AS D_YEAR,
       d.D_YEARMONTH AS D_YEARMONTH
   FROM hive.ssb_1g_csv.lineorder AS l
            INNER JOIN hive.ssb_1g_csv.supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
            INNER JOIN hive.ssb_1g_csv.part AS p ON p.P_PARTKEY = l.LO_PARTKEY
            INNER JOIN hive.ssb_1g_csv.dates AS d ON l.LO_ORDERDATE = d.D_DATEKEY;
```

### Case Two: Accelerate aggregations and aggregations on joins in data lake

Materialized views can be used to accelerate aggregation queries, whether they are on a single table or involve multiple tables.

- Single-table aggregation query

  For typical queries on a single table, their query profile will reveal that the AGGREGATE node consumes a significant amount of time. You can use common aggregation operators to construct materialized views.

  Suppose the following is a slow query:

  ```SQL
  --Q4
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate
  ORDER BY lo_orderdate limit 100;
  ```

  Q4 calculates the daily count of unique orders. Because the count distinct calculation can be computationally expensive, you can create the following two types of materialized views to accelerate:

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_1 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, count(distinct lo_orderkey)
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  
  CREATE MATERIALIZED VIEW mv_2_2 
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  -- lo_orderkey must be the BIGINT type so that it can be used for query rewrite.
  lo_orderdate, bitmap_union(to_bitmap(lo_orderkey))
  FROM hive.ssb_1g_csv.lineorder
  GROUP BY lo_orderdate;
  ```

  Please note that, in this context, do not create materialized views with LIMIT and ORDER BY clauses to avoid rewrite failures. For more information on the query rewrite limitations, see [Query rewrite with materialized views - Limitations](./query_rewrite_with_materialized_views.md#limitations).

- Multi-table aggregation query

  In scenarios involving aggregations of join results, you can create nested materialized views on existing materialized views that join the tables to further aggregate the join results. For example, based on the example in Case One, you can create the following materialized view to accelerate Q1 and Q2 because their aggregation patterns are similar:

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_3
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  AS 
  SELECT
  lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM lineorder_flat_mv
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

  Of course, it is possible to perform both join and aggregation calculations within a single materialized view. While these types of materialized views may have fewer opportunities for query rewrite (due to their specific calculations), they typically occupy less storage space after aggregation. Your choice can be based on your specific use case.

  ```SQL
  CREATE MATERIALIZED VIEW mv_2_4
  DISTRIBUTED BY HASH(lo_orderdate)
  PARTITION BY LO_ORDERDATE
  REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
  PROPERTIES (
      "force_external_table_query_rewrite" = "TRUE"
  )
  AS
  SELECT lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth, SUM(lo_extendedprice * lo_discount) AS REVENUE
  FROM hive.ssb_1g_csv.lineorder, hive.ssb_1g_csv.dates
  WHERE lo_orderdate = d_datekey
  GROUP BY lo_orderdate, lo_discount, lo_quantity, d_year, d_yearmonth;
  ```

### Case Three: Accelerate joins on aggregations in data lake

In some scenarios, it may be necessary to perform aggregation calculations on one table first and then perform join queries with other tables. To fully use StarRocks' query rewrite capabilities, we recommend constructing nested materialized views. For example:

```SQL
--Q5
SELECT * FROM  (
    SELECT 
      l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region, sum(l.lo_revenue)
    FROM 
      hive.ssb_1g_csv.lineorder l 
      INNER JOIN (
        SELECT distinct c_custkey, c_region 
        from 
          hive.ssb_1g_csv.customer 
        WHERE 
          c_region IN ('ASIA', 'AMERICA') 
      ) c ON l.lo_custkey = c.c_custkey
      GROUP BY  l.lo_orderkey, l.lo_orderdate, c.c_custkey, c_region
  ) c1 
WHERE 
  lo_orderdate = '19970503';
```

Q5 first performs an aggregation query on the `customer` table and then performs a join and aggregation with the `lineorder` table. Similar queries might involve different filters on `c_region` and `lo_orderdate`. To leverage query rewrite capabilities, you can create two materialized views, one for aggregation and another for the join.

```SQL
--mv_3_1
CREATE MATERIALIZED VIEW mv_3_1
DISTRIBUTED BY HASH(c_custkey)
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT distinct c_custkey, c_region from hive.ssb_1g_csv.customer; 

--mv_3_2
CREATE MATERIALIZED VIEW mv_3_2
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
PROPERTIES (
    "force_external_table_query_rewrite" = "TRUE"
)
AS
SELECT l.lo_orderdate, l.lo_orderkey, mv.c_custkey, mv.c_region, sum(l.lo_revenue)
FROM hive.ssb_1g_csv.lineorder l 
INNER JOIN mv_3_1 mv
ON l.lo_custkey = mv.c_custkey
GROUP BY l.lo_orderkey, l.lo_orderdate, mv.c_custkey, mv.c_region;
```

### Case Four: Separate hot and cold data for real-time data and historical data in data lake

Consider the following scenario: data updated within the past three days is directly written into StarRocks, while less recent data is checked and batch-written into Hive. However, there are still queries that may involve data from the past seven days. In this case, you can create a simple model with materialized views to expire the data automatically.

```SQL
CREATE MATERIALIZED VIEW mv_4_1 
DISTRIBUTED BY HASH(lo_orderdate)
PARTITION BY LO_ORDERDATE
REFRESH ASYNC EVERY(INTERVAL 1 DAY) 
AS 
SELECT lo_orderkey, lo_orderdate, lo_revenue
FROM hive.ssb_1g_csv.lineorder
WHERE lo_orderdate<=current_date()
AND lo_orderdate>=date_add(current_date(), INTERVAL -4 DAY);
```

You can further build views or materialized views upon it based on the logic of the upper-layer applications.
