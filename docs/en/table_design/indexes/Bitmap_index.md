---
displayed_sidebar: docs
---

# Bitmap indexes

This topic describes how to create and manage a bitmap index, along with usage cases.

## Introduction

A bitmap index is a special database index that uses bitmaps, which are an array of bits. A bit is always in one of two values: 0 and 1. Each bit in the bitmap corresponds to a single row in the table. The value of each bit depends upon the value of the corresponding row.

A bitmap index can help improve the query performance on a given column. If a query's filter conditions match a prefix index, it can significantly improve query efficiency and quickly return results. However, a table can only have one prefix index. If the query's filter conditions do not include the prefix of the prefix index, a bitmap index can be created for that column to improve query efficiency.

### How to design a bitmap index to accelerate queries

The primary considerations for choosing a bitmap index are **column cardinality and the filtering effect of the bitmap index on queries**. Contrary to popular belief, bitmap indexes in StarRocks are more suitable for **queries on columns with high cardinality and queries on the combination of multiple low cardinality columns. Furthermore, bitmap indexes need to effectively filter out data, potentially filtering out at least 999/1000 of the data, thereby reducing the amount of Page data read.**

For queries on a single low cardinality column, the filtering effect of a bitmap index is poor, because too many rows need to be read and scattered across multiple Pages.

:::info

You need to consider the cost of loading data when evaluating the filtering effect of bitmap indexes on queries. In StarRocks, underlying data is organized and loaded by Pages (default size is 64K). The cost of loading data includes the time to load Pages from disk, decompress Pages, and decode.

:::

However, excessively high cardinality can also cause issues such as **occupying more disk space**, and because bitmap indexes need to be constructed during data loading, frequent data loading can **affect loading performance**.

Additionally, the **overhead of loading bitmap indexes during queries** should be considered. During a query, bitmap indexes are loaded on demand, and the larger the value of `number of column values involved in query conditions/cardinality x bitmap index`, the greater the overhead of loading bitmap indexes during queries.

To determine the appropriate cardinality and query conditions for bitmap indexes, it is recommended to refer to the [Performance test on bitmap index](#performance-test-on-bitmap-index) in this topic to conduct performance tests. You can use actual business data and queries to **create bitmap indexes on columns of different cardinalities, to analyze the filtering effect of bitmap indexes on queries (at least filtering out 999/1000 of the data), the disk space usage, the impact on loading performance, and the overhead of loading bitmap indexes during queries.**

StarRocks has a built-in [adaptive selection mechanism for bitmap indexes](#adaptive-selection-of-bitmap-indexes). If a bitmap index fails to accelerate queries, for example, if it cannot filter out many Pages, or the overhead of loading bitmap indexes during queries is high, it will not be used during the query, so query performance will not be significantly affected.

### Adaptive selection of bitmap indexes

StarRocks can adaptively choose whether to use a bitmap index based on column cardinality and query conditions. If a bitmap index does not effectively filter out many Pages or the overhead of loading bitmap indexes during queries is high, StarRocks will not use the bitmap index by default to avoid degrading query performance.

StarRocks determines whether to use a bitmap index based on the ratio of the number of values involved in the query condition to the column cardinality. Generally, the smaller this ratio, the better the filtering effect of the bitmap index. Thus, StarRocks uses `bitmap_max_filter_ratio/1000` as the threshold. When the `number of values in the filter condition/column cardinality` is less than `bitmap_max_filter_ratio/1000`, the bitmap index will be used. The default value of `bitmap_max_filter_ratio` is `1`.

Take a query based on a single column as an example, such as `SELECT * FROM employees WHERE gender = 'male';`. The `gender` column in the `employees` table has values 'male' and 'female', so the cardinality is 2 (two distinct values). The query condition involves one value, so the ratio is 1/2, which is greater than 1/1000. Therefore, this query will not use the bitmap index.

Take another query based on a combination of multiple columns as an example, such as `SELECT * FROM employees WHERE gender = 'male' AND city IN ('Beijing', 'Shanghai');`. The cardinality of the `city` column is of 10,000, and the query condition involves two values, so the ratio is calculated as `(1*2)/(2*10000)`, which is less than 1/1000. Therefore, this query will use the bitmap index.

:::info

The value range for `bitmap_max_filter_ratio` is 1-1000. If `bitmap_max_filter_ratio` is set to `1000`, any query on a column with a bitmap index will be forced to use the bitmap index.

:::

### Advantages

- Bitmap indexes can quickly locate the row numbers of queried column values, suitable for point queries or small-range queries.
- Bitmap indexes can  optimize multi-dimensional queries involving union and intersection operations (OR and AND operations).

## Considerations

### Queries that can be optimized

Bitmap indexes are suitable for optimizing equality `=` queries, `[NOT] IN` range queries, `>`, `>=`, `<`, `<=` queries, and `IS NULL` queries. They are not suitable for optimizing `!=` and `[NOT] LIKE` queries.

### Supported columns and data types

Bitmap indexes can be created on all columns in primary key and duplicate key tables, and on key columns in aggregate and unique key tables. Bitmap indexes can be created on the columns of the following data types:

- Date types: DATE, DATETIME.
- Numeric types: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DECIMAL, BOOLEAN.
- String types: CHAR, STRING, VARCHAR.
- Other types: HLL.

## Basic operations

### Create an index

- Create a bitmap index during table creation.

    ```SQL
    CREATE TABLE `lineorder_partial` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
       INDEX lo_orderdate_index (lo_orderdate) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

    In this example, a bitmap index named `lo_orderdate_index` is created on the `lo_orderdate` column. Naming requirements for bitmap indexes can be found in [System Limits](../../sql-reference/System_limit.md). Identical bitmap indexes cannot be created within the same table.

    Multiple bitmap indexes can be created for multiple columns, separated by commas (,).

    :::note


    For more parameters of table creation, refer to [CREATE TABLE](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md).

    :::

- `CREATE INDEX` can be used to create a bitmap index after table creation. For detailed parameter descriptions and examples, refer to [CREATE INDEX](../../sql-reference/sql-statements/table_bucket_part_index/CREATE_INDEX.md).

    ```SQL
    CREATE INDEX lo_quantity_index (lo_quantity) USING BITMAP;
    ```

### Progress of creating an index

Creating a bitmap index is an asynchronous process. After executing the index creation statement, you can check the index creation progress using the [SHOW ALTER TABLE](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_ALTER.md) command. When the `State` field in the returned value shows `FINISHED`, the index is successfully created.

```SQL
SHOW ALTER TABLE COLUMN;
```

:::info

Each table can only have one ongoing Schema Change task at a time. You cannot create a new bitmap index until the current bitmap index is created.

:::

### View an index

View all bitmap indexes for a specified table. For detailed parameters and returned results, refer to [SHOW INDEX](../../sql-reference/sql-statements/table_bucket_part_index/SHOW_INDEX.md).

```SQL
SHOW INDEXES FROM lineorder_partial;
```

:::note

Creating a bitmap index is an asynchronous process. Using the above statement, you can only view indexes that have been created successfully.

:::

### Delete an index

Delete a bitmap index for a specified table. For detailed parameters and examples, refer to [DROP INDEX](../../sql-reference/sql-statements/table_bucket_part_index/DROP_INDEX.md).

```SQL
DROP INDEX lo_orderdate_index ON lineorder_partial;
```

### Verify whether the bitmap index accelerates queries

Check the `BitmapIndexFilterRows` field in the query Profile. For information on viewing the Profile, refer to [Query analysis](../../administration/Query_planning.md).

## Performance test on bitmap index

### Objective of testing

To analyze the filtering effect and other impacts, such as disk usage, of bitmap indexes on queries with different cardinalities:

- [Query on a single column of low cardinality](#query-1-query-on-single-column-of-low-cardinality)
- [Query on the combination of multiple columns of low cardinality](#query-2-query-on-the-combination-of-multiple-columns-of-low-cardinality)
- [Query on a single column of high cardinality](#query-3-query-based-on-single-high-cardinality-column)

This section also compares the performance between always using bitmap index and using bitmap indexes adaptively to verify the effectiveness of [StarRocks' adaptive selection of bitmap indexes](#adaptive-selection-of-bitmap-indexes).

### Create table and bitmap index

:::warning

To avoid caching Page data affecting query performance, ensure that the BE configuration item `disable_storage` is set to `true`.

:::

This section takes the table `lineorder` (SSB 20G) as an example.

- Original table (without bitmap indexes) as a reference

    ```SQL
    CREATE TABLE `lineorder_without_index` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_linenumber` int(11) NOT NULL COMMENT "",
    `lo_custkey` int(11) NOT NULL COMMENT "",
    `lo_partkey` int(11) NOT NULL COMMENT "",
    `lo_suppkey` int(11) NOT NULL COMMENT "",
    `lo_orderdate` int(11) NOT NULL COMMENT "",
    `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
    `lo_shippriority` int(11) NOT NULL COMMENT "",
    `lo_quantity` int(11) NOT NULL COMMENT "",
    `lo_extendedprice` int(11) NOT NULL COMMENT "",
    `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
    `lo_discount` int(11) NOT NULL COMMENT "",
    `lo_revenue` int(11) NOT NULL COMMENT "",
    `lo_supplycost` int(11) NOT NULL COMMENT "",
    `lo_tax` int(11) NOT NULL COMMENT "",
    `lo_commitdate` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

- Table with bitmap indexes: Bitmap indexes created based on `lo_shipmode`, `lo_quantity`, `lo_discount`, `lo_orderdate`, `lo_tax`, and `lo_partkey`.

    ```SQL
    CREATE TABLE `lineorder_with_index` (
      `lo_orderkey` int(11) NOT NULL COMMENT "",
      `lo_linenumber` int(11) NOT NULL COMMENT "",
      `lo_custkey` int(11) NOT NULL COMMENT "",
      `lo_partkey` int(11) NOT NULL COMMENT "",
      `lo_suppkey` int(11) NOT NULL COMMENT "",
      `lo_orderdate` int(11) NOT NULL COMMENT "",
      `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
      `lo_shippriority` int(11) NOT NULL COMMENT "",
      `lo_quantity` int(11) NOT NULL COMMENT "",
      `lo_extendedprice` int(11) NOT NULL COMMENT "",
      `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
      `lo_discount` int(11) NOT NULL COMMENT "",
      `lo_revenue` int(11) NOT NULL COMMENT "",
      `lo_supplycost` int(11) NOT NULL COMMENT "",
      `lo_tax` int(11) NOT NULL COMMENT "",
      `lo_commitdate` int(11) NOT NULL COMMENT "",
      `lo_shipmode` varchar(11) NOT NULL COMMENT "",
      INDEX i_shipmode (`lo_shipmode`) USING BITMAP,
      INDEX i_quantity (`lo_quantity`) USING BITMAP,
      INDEX i_discount (`lo_discount`) USING BITMAP,
      INDEX i_orderdate (`lo_orderdate`) USING BITMAP,
      INDEX i_tax (`lo_tax`) USING BITMAP,
      INDEX i_partkey (`lo_partkey`) USING BITMAP
    ) ENGINE=OLAP 
    DUPLICATE KEY(`lo_orderkey`)
    DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 1;
    ```

### Disk space usage of bitmap indexes

- `lo_shipmode`: String type, cardinality 7, occupies 130M
- `lo_quantity`: Integer type, cardinality 50, occupies 291M
- `lo_discount`: Integer type, cardinality 11, occupies 198M
- `lo_orderdate`: Integer type, cardinality 2406, occupies 191M
- `lo_tax`: Integer type, cardinality 9, occupies 160M
- `lo_partkey`: Integer type, cardinality 600,000, occupies 601M

### Query 1: Query on single column of low cardinality 

#### Query table without bitmap index

**Query**:

```SQL
SELECT count(1) FROM lineorder_without_index WHERE lo_shipmode="MAIL";
```

**Query performance analysis**: Since the table queried does not have bitmap index, all pages containing the `lo_shipmode` column data need to be read and then predicate filtering is applied.

Total Time: Approximately 0.91 milliseconds, **with data loading taking 0.47 milliseconds**, decoding dictionary for low cardinality optimization taking 0.31 milliseconds, and predicate filtering taking 0.23 milliseconds.

```Bash
PullRowNum: 20.566M (20566493) // Number of rows in the result set.
CompressedBytesRead: 55.283 MB // Total amount of data read.
RawRowsRead: 143.999M (143999468) // Number of rows read. Since there is no bitmap index, all data in this column is read.
ReadPagesNum: 8.795K (8795) // Number of pages read. Since there is no bitmap index, all pages containing data for this column are read.
IOTaskExecTime: 914ms // Total time for scanning data.
    BlockFetch: 469ms // Time for loading data.
    DictDecode: 311.612ms // Time for decoding dictionary for low cardinality optimization.
    PredFilter: 23.182ms // Time for predicate filtering.
    PredFilterRows: 123.433M (123432975) // Number of rows filtered out.
```

#### Query table with bitmap index

##### Use bitmap index compulsorily

:::info

To use the bitmap index compulsorily, according to Starrocks' configuration,  `bitmap_max_filter_ratio=1000` needs to be set in the **be.conf** file of each BE node, and then the BE nodes need to be restarted.

:::

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**Query performance analysis**: Since the column queried is of low cardinality, bitmap index does not filter the data efficiently. Even though bitmap index can quickly locate the row numbers of actual data, a large number of rows need to be read, scattered across multiple pages. As a result, it cannot effectively filter out the pages that need to be read. Moreover, additional overhead for loading the bitmap index and using the bitmap index to filter data is incurred, resulting in a longer total time.

Total time: 2.7 seconds, **with 0.93 seconds spent loading data and bitmap index**, 0.33 seconds on decoding dictionary for low cardinality optimization, 0.42 seconds on filtering data with bitmap index, and 0.17 seconds on filtering data with ZoneMap Index.

```Bash
PullRowNum: 20.566M (20566493) // Number of rows in the result set.
CompressedBytesRead: 72.472 MB // Total amount of data read. The size of the bitmap index for this column is 130MB, with 7 unique values. The size of a bitmap index for each value is 18MB. The data size of Pages is 55MB = 73MB.
RawRowsRead: 20.566M (20566493) // Number of rows read. 20 million rows are actually read.
ReadPagesNum: 8.802K (8802) // Number of Pages read. All Pages are read because the 20 million rows filtered by bitmap index are randomly distributed across different Pages, and Page is the smallest data reading unit. Therefore, bitmap index does not filter out Pages.
IOTaskExecTime: 2s77ms // Total time for scanning data, longer than without bitmap index.
    BlockFetch: 931.144ms // Time for loading data and bitmap index. Compared to the previous query, an additional 400ms was spent to load bitmap index (18MB).
    DictDecode: 329.696ms // Time for decoding dictionary for low cardinality optimization is similar since the output row count is the same.
    BitmapIndexFilter: 419.308ms // Time for filtering data with the bitmap index.
    BitmapIndexFilterRows: 123.433M (123432975) // Number of rows filtered by the bitmap index.
    ZoneMapIndexFiter: 171.580ms // Time for filtering data with ZoneMap Index.
```

##### Determine whether to use bitmap index based on StarRocks default configuration

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL";
```

**Query performance analysis**: According to StarRocks default configuration, bitmap index is used only if the number of distinct values in the filter condition column / column cardinality < `bitmap_max_filter_ratio/1000` (default is 1/1000). In this case, the value is greater than 1/1000, so bitmap index is not used, resulting in performance similar to querying a table without bitmap index.

```Bash
PullRowNum: 20.566M (20566493) // Number of rows in the result set.
CompressedBytesRead: 55.283 MB // Total amount of data read.
RawRowsRead: 143.999M (143999468) // Number of rows read. Since the bitmap index is not used, all data in this column is read.
ReadPagesNum: 8.800K (8800) // Number of Pages read. Since the bitmap index is not used, all pages containing data for this column are read.
IOTaskExecTime: 914.279ms // Total time for scanning data.
    BlockFetch: 475.890ms // Time for loading data.
    DictDecode: 312.019ms // Time for decoding dictionary for low cardinality optimization.
    PredFilterRows: 123.433M (123432975) // Number of rows filtered out by predicate.
```

### Query 2: Query on the combination of multiple columns of low cardinality

#### Query table without bitmap index

**Query**:

```SQL
SELECT count(1) 
FROM lineorder_without_index 
WHERE lo_shipmode = "MAIL" 
  AND lo_quantity = 10 
  AND lo_discount = 9 
  AND lo_tax = 8;
```

**Query performance analysis**: Since the table queried does not have any bitmap index, all Pages containing data of `lo_shipmode`, `lo_quantity`, `lo_discount`, and `lo_tax` columns are read and then predicate filtering is applied.

Total time taken: 1.76 seconds, **with 1.6 seconds spent loading data (4 columns)**, and 0.1 seconds on predicate filtering.

```Bash
PullRowNum: 4.092K (4092) // Number of rows in the result set.
CompressedBytesRead: 305.346 MB // Total amount of data read. Data of 4 columns is read, totaling 305MB.
RawRowsRead: 143.999M (143999468) // Number of rows read. Since there is no bitmap index, all data of these 4 columns are read.
ReadPagesNum: 35.168K (35168) // Number of Pages read. Since there is no bitmap index, all Pages containing data of these 4 columns are read.
IOTaskExecTime: 1s761ms // Total time for scanning data.
    BlockFetch: 1s639ms // Time for loading data.
    PredFilter: 96.533ms // Time for predicate filtering with 4 filter conditions.
    PredFilterRows: 143.995M (143995376) // Number of rows filtered out by predicates.
```

#### Query Table with bitmap index

##### Use bitmap index compulsorily

:::info

To use bitmap index compulsorily, according to Starrocks' configuration, the `bitmap_max_filter_ratio=1000` must be set in the **be.conf** file of each BE node, then the BE nodes must be restarted.

:::

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**Query performance analysis**: Since this is a combination query based on multiple low cardinality columns, bitmap index is effective, filtering out a portion of the Pages and significantly reducing data read time.

Total time taken: 0.68 seconds, **with 0.54 seconds spent loading data and bitmap index**, and 0.14 seconds filtering data with bitmap index.

```Bash
PullRowNum: 4.092K (4092) // Number of rows in the result set.
CompressedBytesRead: 156.340 MB // Total amount of data read. The bitmap index effectively filters out 2/3 of the data. Out of this 156MB, 60MB is index data, and 90MB is actual data.
ReadPagesNum: 11.325K (11325) // Number of Pages read. The bitmap index effectively filters out 2/3 of the Pages.
IOTaskExecTime: 683.471ms // Total time for scanning data, significantly less than the total time spent without bitmap index.
    BlockFetch: 537.421ms // Time for loading data and the bitmap index. Although additional time is spent loading the bitmap index, it significantly reduces the time to load data.
    BitmapIndexFilter: 139.024ms // Time for filtering data with bitmap index.
    BitmapIndexFilterRows: 143.995M (143995376) // Number of rows filtered out by bitmap index.
```

##### Determine whether to use bitmap index based on StarRocks default configuration

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_shipmode="MAIL" AND lo_quantity=10 AND lo_discount=9 AND lo_tax=8;
```

**Query performance analysis**: According to StarRocks default configuration, the bitmap index is used if the number of distinct values in the filter condition columns / column cardinality < `bitmap_max_filter_ratio/1000` (default is 1/1000). In this case, the value is less than 1/1000, so the bitmap index is used, resulting in performance similar to use the bitmap index compulsorily.

Total time taken: 0.67 seconds, **with 0.54 seconds spent loading data and bitmap index**, and 0.13 seconds filtering data with bitmap index.

```Bash
PullRowNum: 4.092K (4092) // Number of rows in the result set.
CompressedBytesRead: 154.430 MB // Total amount of data read.
ReadPagesNum: 11.209K (11209) // Number of Pages read.
IOTaskExecTime: 672.029ms // Total time for scanning data, significantly less than without bitmap index.
    BlockFetch: 535.823ms // Time for loading data and bitmap index. Although loading bitmap index takes a little more time, it greatly reduces data loading time.
    BitmapIndexFilter: 128.403ms // Time for filtering data with bitmap index.
    BitmapIndexFilterRows: 143.995M (143995376) // Number of rows filtered out by bitmap index.
```

### Query 3: Query on single Column of high cardinality 

#### Query table without bitmap index

**Query**:

```SQL
select count(1) from lineorder_without_index where lo_partkey=10000;
```

**Query performance analysis**: Since the table queried does not have a bitmap index, pages containing the `lo_partkey` column data are read and then predicate filtering is applied.

Total time: approximately 0.43 ms, **including 0.39 ms for data loading** and 0.02 ms for predicate filtering.

```Bash
PullRowNum: 255 // Number of rows in the result set.
CompressedBytesRead: 344.532 MB // Total amount of data read.
RawRowsRead: 143.999M (143,999,468) // Number of rows read. Since  there is no bitmap index, all data in this column is read.
ReadPagesNum: 8.791K (8,791) // Number of pages read. Since there is no bitmap index, all data of this column are read.
IOTaskExecTime: 428.258ms // Total time for scanning data.
    BlockFetch: 392.434ms // Time for loading data.
    PredFilter: 20.807ms // Time for predicate filtering.
    PredFilterRows: 143.999M (143,999,213) // Number of rows filtered out by predicates.
```

#### Query a Table with bitmap index

##### Use bitmap index compulsorily

:::info

To use the bitmap index compulsorily, according to StarRocks' configuration, you must set `bitmap_max_filter_ratio=1000` in the **be.conf** file on each BE node, and then restart the BE nodes.

:::

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**Query performance analysis**: Since the queried column is of high cardinality, the bitmap index is effective, allowing for filtering out a portion of the pages and significantly reducing the time for reading data.

Total time: 0.015 seconds, **including 0.009 seconds for loading data and bitmap index**, and 0.003 seconds for bitmap index filtering.

```Bash
PullRowNum: 255 // Number of rows in the result set.
CompressedBytesRead: 13.600 MB // Total amount of data read. Bitmap Index filtering is effective, filtering out a large amount of data.
RawRowsRead: 255 // Number of rows read.
ReadPagesNum: 225 // Number of pages read. The bitmap index can effectively filter out data, filtering out a large number of pages.
IOTaskExecTime: 15.354ms // Total time for scanning data, significantly less than the total time spent without Bitmap Index.
    BlockFetch: 9.530ms // Time for loading data and Bitmap Index. Although additional time is spent loading the Bitmap Index, it significantly reduces the time to load data.
    BitmapIndexFilter: 3.450ms // Time for filtering data with Bitmap Index.
    BitmapIndexFilterRows: 143.999M (143,999,213) // Number of rows filtered out by Bitmap Index.
```

##### Determine whether to use bitmap index based on StarRocks default configuration

**Query**:

```SQL
SELECT count(1) FROM lineorder_with_index WHERE lo_partkey=10000;
```

**Query performance analysis**: According to StarRocks' default configuration, Bitmap Index is used when the number of distinct values/column cardinality < `bitmap_max_filter_ratio/1000` (default 1/1000). Since this condition is met, the query uses the Bitmap Index, and the performance is similar to that when the Bitmap Index is compulsorily used..

Total time: 0.014 seconds, including **0.008 seconds for loading data and bitmap index**, and 0.003 seconds for Bitmap Index filtering.

```Bash
PullRowNum: 255 // Number of rows in the result set.
CompressedBytesRead: 13.600 MB // Total amount of data read. The bitmap index can effectively filter out data, filtering out a large number of data.
RawRowsRead: 255 // Number of rows read.
ReadPagesNum: 225 // Number of pages read. The bitmap index can effectively filter out data, filtering out a large number of pages.
IOTaskExecTime: 14.387ms // Total time for scanning data, significantly less than the total time spent without Bitmap Index.
    BitmapIndexFilter: 2.979ms // Time for filtering data with Bitmap Index.
    BitmapIndexFilterRows: 143.999M (143,999,213) // Number of rows filtered out by Bitmap Index.
    BlockFetch: 8.988ms // Time for loading data and Bitmap Index. Although additional time is spent loading the Bitmap Index, it significantly reduces the time to load data.
```
