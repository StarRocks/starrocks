# Aggregate table

When you create a table that uses the Aggregate table, you can define sort key columns and metric columns and can specify an aggregate function for the metric columns. If the records to be loaded have the same sort key, the metric columns are aggregated. The Aggregate table helps reduce the amount of data that needs to be processed for queries, thereby accelerating queries.

## Scenarios

The Aggregate table is well suited to data statistics and analytics scenarios. A few examples are as follows:

- Help website or app providers analyze the amount of traffic and time that their users spend on a specific website or app and the total number of visits to the website or app.

- Help advertising agencies analyze the total clicks, total views, and consumption statistics of an advertisement that they provide for their customers.

- Help e-commerce companies analyze their annual trading data to identify the geographic bestsellers within individual quarters or months.

The data querying and ingestion in the preceding scenarios have the following characteristics:

- Most queries are aggregate queries, such as SUM, MAX, and MIN.
- Raw detailed data does not need to be retrieved.
- Historical data is not frequently updated. Only new data is appended.

## Principle

Starting from data ingestion to data querying, data with the same sort key in a table that uses the Aggregate table is aggregated multiple times as follows:

1. In the data ingestion phase, when data is loaded as batches into the table, each batch comprises a data version. After a data version is generated, StarRocks aggregates the data that has the same sort key in the data version.
2. In the background compaction phase, when the files of multiple data versions that are generated at data ingestion are periodically compacted into a large file, StarRocks aggregates the data that has the same sort key in the large file.
3. In the data query phase, StarRocks aggregates the data that has the same sort key among all data versions before it returns the query result.

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

Suppose that you want to analyze the numbers of visits by users from different cities to different web pages. In this example, create a table named `example_db.aggregate_tbl`, define `site_id`, `date`, and `city_code` as sort key columns, define `pv` as a metric column, and specify the SUM function for the `pv` column.

The statement for creating the table is as follows:

```SQL
CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (
    site_id LARGEINT NOT NULL COMMENT "id of site",
    date DATE NOT NULL COMMENT "time of event",
    city_code VARCHAR(20) COMMENT "city_code of user",
    pv BIGINT SUM DEFAULT "0" COMMENT "total page views"
)
AGGREGATE KEY(site_id, date, city_code)
DISTRIBUTED BY HASH(site_id)
PROPERTIES (
"replication_num" = "3"
);
```

> **NOTICE**
>
> - When you create a table, you must specify the bucketing column by using the `DISTRIBUTED BY HASH` clause. For detailed information, see [bucketing](../Data_distribution.md#design-partitioning-and-bucketing-rules).
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../Data_distribution.md#determine-the-number-of-buckets).

## Usage notes

- Take note of the following points about the sort key of a table:
  - You can use the `AGGREGATE KEY` keyword to explicitly define the columns that are used in the sort key.

    - If the `AGGREGATE KEY` keyword does not include all the dimension columns, the table cannot be created.
    - By default, if you do not explicitly define sort key columns by using the `AGGREGATE KEY` keyword, StarRocks selects all columns except metric columns as the sort key columns.

  - The sort key must be created on columns on which unique constraints are enforced. It must be composed of all the dimension columns whose names cannot be changed.

- You can specify an aggregate function following the name of a column to define the column as a metric column. In most cases, metric columns hold data that needs to be aggregated and analyzed.

- For information about the aggregate functions that are supported by the Aggregate table, see [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

- When queries are run, sort key columns are filtered before the aggregation of multiple data versions, whereas metric columns are filtered after the aggregation of multiple data versions. Therefore, we recommend that you identify the columns that are frequently used as filter conditions and define these columns as the sort key. This way, data filtering can start before the aggregation of multiple data versions to improve query performance.

- When you create a table, you cannot create BITMAP indexes or Bloom Filter indexes on the metric columns of the table.

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Data import](../../loading/Loading_intro.md).

> Note: When you load data into a table that uses the Aggregate table, you can only update all columns of the table. For example, when you update the preceding `example_db.aggregate_tbl` table, you must update all its columns, which are `site_id`, `date`, `city_code`, and `pv`.
