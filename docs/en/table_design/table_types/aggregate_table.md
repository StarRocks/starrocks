---
displayed_sidebar: docs
sidebar_position: 40
---

# Aggregate table

<<<<<<< HEAD
When you create a table that uses the Aggregate table, you can define sort key columns and metric columns and can specify an aggregate function for the metric columns. If the records to be loaded have the same sort key, the metric columns are aggregated. The Aggregate table helps reduce the amount of data that needs to be processed for queries, thereby accelerating queries.
=======
At table creation, you can define an aggregate key and specify an aggregate function for the value column. When multiple rows of data have the same aggregate key, the values in the value columns are aggregated. Additionally, you can define the sort key separately. If the filter conditions in queries include the sort key, StarRocks can quickly filter the data, improving query efficiency.

In data analysis and aggregation scenarios, Aggregate tables can reduce the amount of data that needs to be processed, thereby enhancing query efficiency.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## Scenarios

The Aggregate table is well suited to data statistics and analytics scenarios. A few examples are as follows:

- Help website or app providers analyze the amount of traffic and time that their users spend on a specific website or app and the total number of visits to the website or app.

- Help advertising agencies analyze the total clicks, total views, and consumption statistics of an advertisement that they provide for their customers.

- Help e-commerce companies analyze their annual trading data to identify the geographic bestsellers within individual quarters or months.

<<<<<<< HEAD
The data querying and ingestion in the preceding scenarios have the following characteristics:
=======
The data query and ingestion in the preceding scenarios have the following characteristics:
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

- Most queries are aggregate queries, such as SUM, MAX, and MIN.
- Raw detailed data does not need to be retrieved.
- Historical data is not frequently updated. Only new data is appended.

## Principle

<<<<<<< HEAD
Starting from data ingestion to data querying, data with the same sort key in a table that uses the Aggregate table is aggregated multiple times as follows:

1. In the data ingestion phase, when data is loaded as batches into the table, each batch comprises a data version. After a data version is generated, StarRocks aggregates the data that has the same sort key in the data version.
2. In the background compaction phase, when the files of multiple data versions that are generated at data ingestion are periodically compacted into a large file, StarRocks aggregates the data that has the same sort key in the large file.
3. In the data query phase, StarRocks aggregates the data that has the same sort key among all data versions before it returns the query result.
=======
From the phrase of data ingestion to data query, data in the Aggregate tables is aggregated multiple times as follows:

1. In the data ingestion phase, each batch of data forms a version when data is loaded into the Aggregate table in batches. In one version, data with the same aggregate key will be aggregated.

2. In the background compaction phase, when the files of multiple data versions that are generated at data ingestion are periodically compacted into a large file, StarRocks aggregates the data that has the same aggregate key in the large file.
3. In the data query phase, StarRocks aggregates the data that has the same aggregate key among all data versions before it returns the query result.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

The aggregate operations help reduce the amount of data that needs to be processed, thereby accelerating queries.

Suppose that you have a table that uses the Aggregate table and want to load the following four raw records into the table.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 1    |
| 2020.05.01 | CHN     | 2    |
| 2020.05.01 | USA     | 3    |
| 2020.05.01 | USA     | 4    |

StarRocks aggregates the four raw records into the following two records at data ingestion.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 3    |
| 2020.05.01 | USA     | 7    |

## Create a table

<<<<<<< HEAD
Suppose that you want to analyze the numbers of visits by users from different cities to different web pages. In this example, create a table named `example_db.aggregate_tbl`, define `site_id`, `date`, and `city_code` as sort key columns, define `pv` as a metric column, and specify the SUM function for the `pv` column.
=======
Suppose that you want to analyze the numbers of visits by users from different cities to different web pages. In this example, create a table named `example_db.aggregate_tbl`, define `site_id`, `date`, and `city_code` as the aggregate key, define `pv` as a value column, and specify the SUM function for the `pv` column.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

The statement for creating the table is as follows:

```SQL
<<<<<<< HEAD
CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (
=======
CREATE TABLE aggregate_tbl (
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    site_id LARGEINT NOT NULL COMMENT "id of site",
    date DATE NOT NULL COMMENT "time of event",
    city_code VARCHAR(20) COMMENT "city_code of user",
    pv BIGINT SUM DEFAULT "0" COMMENT "total page views"
)
AGGREGATE KEY(site_id, date, city_code)
<<<<<<< HEAD
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
"replication_num" = "3"
);
=======
DISTRIBUTED BY HASH(site_id);
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
```

> **NOTICE**
>
> - When you create a table, you must specify the bucketing column by using the `DISTRIBUTED BY HASH` clause. For detailed information, see [bucketing](../data_distribution/Data_distribution.md#bucketing).
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../data_distribution/Data_distribution.md#set-the-number-of-buckets).

## Usage notes

<<<<<<< HEAD
- Take note of the following points about the sort key of a table:
  - You can use the `AGGREGATE KEY` keyword to explicitly define the columns that are used in the sort key.

    - If the `AGGREGATE KEY` keyword does not include all the dimension columns, the table cannot be created.
    - By default, if you do not explicitly define sort key columns by using the `AGGREGATE KEY` keyword, StarRocks selects all columns except metric columns as the sort key columns.

  - The sort key must be created on columns on which unique constraints are enforced. It must be composed of all the dimension columns whose names cannot be changed.

- You can specify an aggregate function following the name of a column to define the column as a metric column. In most cases, metric columns hold data that needs to be aggregated and analyzed.

- For information about the aggregate functions that are supported by the Aggregate table, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

- When queries are run, sort key columns are filtered before the aggregation of multiple data versions, whereas metric columns are filtered after the aggregation of multiple data versions. Therefore, we recommend that you identify the columns that are frequently used as filter conditions and define these columns as the sort key. This way, data filtering can start before the aggregation of multiple data versions to improve query performance.

- When you create a table, you cannot create BITMAP indexes or Bloom Filter indexes on the metric columns of the table.
=======
- **Aggregate key**:
  - In the CREATE TABLE statement, the aggregate key must be defined before other columns.
  - The aggregate key can be explicitly defined using `AGGREGATE KEY`. The `AGGREGATE KEY` must include all columns except the value columns, otherwise the table fails to be created.

    If the aggregate key is not explicitly defined using `AGGREGATE KEY`, all columns except the value columns are considered as the aggregate key by default.
  - The aggregate key has uniqueness constraint.

- **Value column**: Define a column as the value column by specifying an aggregate function after the column name. This column generally holds data that needs to be aggregated.

- **Aggregate function**: The aggregate function used for the value column. For supported aggregate functions for the Aggregate tables, see [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

- **Sort key**

  - Since v3.3.0, the sort key is decoupled from the aggregate key in the Aggregate table. The Aggregate table supports specifying the sort key using `ORDER BY` and specifying the aggregate key using `AGGREGATE KEY`. The columns in the sort key and the aggregate key need to be the same, but the order of the columns does not need to be the same.

  - When queries are run, sort key columns are filtered before the aggregation of multiple data versions, whereas value columns are filtered after the aggregation of multiple data versions. Therefore, we recommend that you identify the columns that are frequently used as filter conditions and define these columns as the sort key. This way, data filtering can start before the aggregation of multiple data versions to improve query performance.

- When you create a table, you can only create Bitmap indexes or Bloom Filter indexes on the key columns of the table.
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Loading options](../../loading/Loading_intro.md).

> Note: When you load data into a table that uses the Aggregate table, you can only update all columns of the table. For example, when you update the preceding `example_db.aggregate_tbl` table, you must update all its columns, which are `site_id`, `date`, `city_code`, and `pv`.
