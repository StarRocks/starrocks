---
displayed_sidebar: "English"
---

# Sort keys and prefix indexes

When you create a table, you can select one or more of its columns to comprise a sort key. The sort key determines the order in which the data of the table is sorted before the data is stored on disk. You can use the sort key columns as filter conditions for queries. As such, StarRocks can quickly locate the data of interest, saving it from scanning the entire table to find the data that it needs to process. This reduces search complexity and therefore accelerates queries.

Additionally, to reduce memory consumption, StarRocks supports creating a prefix index on a table. Prefix indexes are a type of spare index. StarRocks stores every 1024 rows of the table in a block, for which an index entry is generated and stored in the prefix index table. The prefix index entry for a block cannot exceed 36 bytes in length, and its content is the prefix composed of the table's sort key columns in the first row of that block. This helps StarRocks quickly locate the starting column number of the block that stores the data of that row when a search on the prefix index table is run. The prefix index of a table is 1024 times less than the table itself in size. Therefore, the entire prefix index can be cached in memory to help accelerate queries.

## Principles

In the Duplicate Key table, sort key columns are defined by using the `DUPLICATE KEY` keyword.

In the Aggregate table, sort key columns are defined by using the `AGGREGATE KEY` keyword.

In the Unique Key table, sort key columns are defined by using the `UNIQUE KEY` keyword.

Since v3.0, the primary key and sort key are decoupled in the Primary Key table. The sort key columns are defined by using the `ORDER BY` keyword. The primary key columns are defined by using the `PRIMARY KEY` keyword.

When you define sort key columns for a Duplicate Key table, an Aggregate table, or a Unique Key table, take note of the following points:

- Sort key columns must be continuously defined columns, of which the first defined column must be the beginning sort key column.

- The columns that you plan to select as sort key columns must be defined prior to the other common columns.

- The sequence in which you list sort key columns must comply with the sequence in which you define the columns of the table.

The following examples show allowed sort key columns and unallowed sort key columns of a table that consists of four columns, which are `site_id`, `city_code`, `user_id`, and `pv`:

- Examples of allowed sort key columns
  - `site_id` and `city_code`
  - `site_id`, `city_code`, and `user_id`

- Examples of unallowed sort key columns
  - `city_code` and `site_id`
  - `city_code` and `user_id`
  - `site_id`, `city_code`, and `pv`

The following sections provide examples of how to define sort key columns when you create tables of different types. These examples are suitable for StarRocks clusters that have at least three BEs.

### Duplicate Key

Create a table named `site_access_duplicate`. The table consists of four columns: `site_id`, `city_code`, `user_id`, and `pv`, of which `site_id` and `city_code` are selected as sort key columns.

The statement for creating the table is as follows:

```SQL
CREATE TABLE site_access_duplicate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
DUPLICATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

> **NOTICE**
>
> Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](./Data_distribution.md#determine-the-number-of-buckets).

### Aggregate Key

Create a table named `site_access_aggregate`. The table consists of four columns: `site_id`, `city_code`, `user_id`, and `pv`, of which `site_id` and `city_code` are selected as sort key columns.

The statement for creating the table is as follows:

```SQL
CREATE TABLE site_access_aggregate
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id BITMAP BITMAP_UNION,
    pv BIGINT SUM DEFAULT '0'
)
AGGREGATE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

>**NOTICE**
>
> For an Aggregate table, columns for which `agg_type` is not specified are key columns, and those for which `agg_type` is specified are value columns. See [CREATE TABLE](../sql-reference/sql-statements/data-definition/CREATE_TABLE.md). In the preceding example, only `site_id` and `city_code` are specified as sort key columns, and therefore `agg_type` must be specified for `user_id` and `pv`.

### Unique Key

Create a table named `site_access_unique`. The table consists of four columns: `site_id`, `city_code`, `user_id`, and `pv`, of which `site_id` and `city_code` are selected as sort key columns.

The statement for creating the table is as follows:

```SQL
CREATE TABLE site_access_unique
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
UNIQUE KEY(site_id, city_code)
DISTRIBUTED BY HASH(site_id);
```

### Primary Key

Create a table named `site_access_primary`. The table consists of four columns: `site_id`, `city_code`, `user_id`, and `pv`, of which `site_id` is selected as the primary key column, `site_id` and `city_code` are selected as sort key columns.

The statement for creating the table is as follows:

```SQL
CREATE TABLE site_access_primary
(
    site_id INT DEFAULT '10',
    city_code SMALLINT,
    user_id INT,
    pv BIGINT DEFAULT '0'
)
PRIMARY KEY(site_id)
DISTRIBUTED BY HASH(site_id)
ORDER BY(site_id,city_code);
```

## Sorting effect

Use the preceding tables as examples. The sorting effect varies in the following three situations:

- If your query filters on both `site_id` and `city_code`, the number of rows that StarRocks needs to scan during the query is significantly reduced:

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123 and city_code = 2;
  ```

- If your query filters only on `site_id`, StarRocks can narrow the query range down to the rows that contain `site_id` values:

  ```Plain
  select sum(pv) from site_access_duplicate where site_id = 123;
  ```

- If your query filters only on `city_code`, StarRocks needs to scan the entire table:

  ```Plain
  select sum(pv) from site_access_duplicate where city_code = 2;
  ```

  > **NOTE**
  >
  > In this situation, the sort key columns do not yield the expected sorting effect.

As described above, when your query filters on both `site_id` and `city_code`, StarRocks runs a binary search on the table to narrow the query range down to a specific location. If the table consists of a large number of rows, StarRocks runs binary searches on the `site_id` and `city_code` columns instead. This requires StarRocks to load the data of the two columns into memory and therefore increases memory consumption. In this case, you can use a prefix index to reduce the amount of data cached in memory, thereby accelerating your query.

Additionally, note that a large number of sort key columns also increase memory consumption. To reduce memory consumption, StarRocks imposes the following limits on the usage of prefix indexes:

- The prefix index entry of a block must be composed of the prefix of the table's sort key columns in the first row of that block.

- A prefix index can be created on a maximum of 3 columns.

- A prefix index entry cannot exceed 36 bytes in length.

- A prefix index cannot be created on columns of the FLOAT or DOUBLE data type.

- Of all the columns on which a prefix index is created, only one column of the VARCHAR data type is allowed, and that column must be the end column for the prefix index.

- If the end column for a prefix index is of the CHAR or VARCHAR data type, no entries in the prefix index can exceed 36 bytes.

## How to select sort key columns

This section uses the `site_access_duplicate` table as an example to describe how to select sort key columns.

- We recommend that you identify the columns on which your queries frequently filter and select these columns as sort key columns.

- If you select more than one sort key column, we recommend that you list frequently filtered columns of high discrimination levels prior to the other columns.
  
  A column has a high discrimination level if the number of values in the column is large and continuously grows. For example, the number of cities in the `site_access_duplicate` table is fixed, which means that the number of values in the `city_code` column of the table is fixed. However, the number of values in the `site_id` column is much greater than the number of values in the `city_code` column and continuously grows. Therefore, the `site_id` column has a higher discrimination level than the `city_code` column.

- We recommend that you do not select a large number of sort key columns. A large number of sort key columns cannot help improve query performance but increase the overheads for sorting and data loading.

In summary, take note of the following points when you select sort key columns for the `site_access_duplicate` table:

- If your queries frequently filter on both `site_id` and `city_code`, we recommend that you select `site_id` as the beginning sort key column.

- If your queries frequently filter only on `city_code` and occasionally filter on both `site_id` and `city_code`, we recommend that you select `city_code` as the beginning sort key column.

- If the number of times that your queries filter on both `site_id` and `city_code` is roughly equal to the number of times that your queries filter only on `city_code`, we recommend that you create a materialized view, for which the first column is `city_code`. As such, StarRocks creates a sort index on the `city_code` column of the materialized view.
