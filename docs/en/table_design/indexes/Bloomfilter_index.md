---
displayed_sidebar: "English"
---

# Bloom filter indexes

This topic describes how to create and modify bloom filter indexes, along with how they works.

A bloom filter index is a space-efficiency data structure that is used to detect the possible presence of filtered data in data files of a table. If the bloom filter index detects that the data to be filtered are not in a certain data file, StarRocks skips scanning the data file. Bloom filter indexes can reduce response time when the column (such as ID) has a relatively high cardinality.

If a query hits a sort key column, StarRocks efficiently returns the query result by using the [prefix index](./indexes_overview.md#prefix-indexes). However, the prefix index entry for a data block cannot exceed 36 bytes in length. If you want to improve the query performance on a column, which is not used as a sort key and has a relatively high cardinality, you can create a bloom filter index for the column.

## How it works

For example, you create a bloom filter index on a `column1` of a given table `table1` and run a query such as `Select xxx from table1 where column1 = something;`. Then the following situations happen when StarRocks scans the data files of `table1`.

- If the bloom filter index detects that a data file does not contain the data to be filtered, StarRocks skips the data file to improve query performance.
- If the bloom filter index detects that a data file may contain the data to be filtered, StarRocks reads the data file to check whether the data exists. Note that the bloom filter can tell you for sure if a value is not present, but it cannot say for sure that a value is present, only that it may be present. Using a bloom filter index to determine whether a value is present may give false positives, which means that a bloom filter index detects that a data file contains the data to be filtered, but the data file does not actually contain the data.

## Usage notes

- You can create bloom filter indexes for all columns of a Duplicate Key or Primary Key table. For an Aggregate table or Unique Key table, you can only create bloom filter indexes for key columns.
- TINYINT, FLOAT, DOUBLE, and DECIMAL columns do not support creating bloom filter indexes.
- Bloom filter indexes can only improve the performance of queries that contain the `in` and `=` operators, such as `Select xxx from table where x in {}` and `Select xxx from table where column = xxx`.
- You can check whether a query uses bloom filter indexes by viewing the `BloomFilterFilterRows` field of the query's profile.

## Create bloom filter indexes

You can create a bloom filter index for a column when you create a table by specifying the `bloom_filter_columns` parameter in `PROPERTIES`. For example, create bloom filter indexes for the `k1` and `k2` columns in `table1`.

```SQL
CREATE TABLE table1
(
    k1 BIGINT,
    k2 LARGEINT,
    v1 VARCHAR(2048) REPLACE,
    v2 SMALLINT DEFAULT "10"
)
ENGINE = olap
PRIMARY KEY(k1, k2)
DISTRIBUTED BY HASH (k1, k2)
PROPERTIES("bloom_filter_columns" = "k1,k2");
```

You can create bloom filter indexes for multiple columns at a time by specifying these column names. Note that you need to separate these column names with commas (`,`). For other parameter descriptions of the CREATE TABLE statement, see [CREATE TABLE](../../sql-reference/sql-statements/data-definition/CREATE_TABLE.md).

## Display bloom filter indexes

For example, the following statement displays bloom filter indexes of `table1`. For the output description, see [SHOW CREATE TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_CREATE_TABLE.md).

```SQL
SHOW CREATE TABLE table1;
```

## Modify bloom filter indexes

You can add, reduce, and delete bloom filter indexes by using the [ALTER TABLE](../../sql-reference/sql-statements/data-definition/ALTER_TABLE.md) statement.

- The following statement adds a bloom filter index on the `v1` column.

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1,k2,v1");
    ```

- The following statement reduces the bloom filter index on the `k2` column.
  
    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "k1");
    ```

- The following statement deletes all bloom filter indexes of `table1`.

    ```SQL
    ALTER TABLE table1 SET ("bloom_filter_columns" = "");
    ```

> Note: Altering an index is an asynchronous operation. You can view the progress of this operation by executing [SHOW ALTER TABLE](../../sql-reference/sql-statements/data-manipulation/SHOW_ALTER.md). You can run only one alter index task on a table each time.
