---
displayed_sidebar: docs
---

# Prefix indexes

## Description

Specify one or more columns to comprise the sort key at table creation. The data rows in the table will be sorted based on the sort key and then stored on the disk.

**During data writing, the Prefix index is automatically generated. After the data is sorted according to the specified sort key, every 1024 rows of data are included in one logical data block. An index entry that consists of the values of sort key columns of the first data row in that logical data block is added to the Prefix index table.**

With these two layers of sorting structures, queries can use binary search to quickly skip data that does not meet the query conditions, and can also avoid additional sorting operations during queries.

:::tip

The Prefix index is a sparse index, and its size is at least 1024 times smaller than the data volume. Therefore, it can generally be fully cached in memory, to accelerate query performance.

:::

## Usage notes

Since v3.0, Primary Key tables support defining sort keys using `ORDER BY`. Since v3.3, Duplicate Key tables, Aggregate tables, and Unique Key tables support defining sort keys using `ORDER BY`.

- Data in the Duplicate Key table is sorted according to the sort key `ORDER BY`. The sort key can be combination of any columns.
  :::info

  When both `ORDER BY` and `DUPLICATE KEY` are specified, `DUPLICATE KEY` does not take effect.

  :::
- Data in the Aggregate table is first aggregated according to the aggregate key `AGGREGATE KEY`, then sorted according to the sort key `ORDER BY`. The columns in `ORDER BY` and `AGGREGATE KEY` need to be same, but the order of the columns does not need to be same.
- Data in the Unique Key table is first replaced according to the unique key `UNIQUE KEY`, then sorted according to the sort key `ORDER BY`. The columns in `ORDER BY` and `UNIQUE KEY` need to be same, but the order of the columns does not need to be same.
- Data in the Primary Key table is first replaced according to the primary key `PRIMARY KEY`, then sorted according to the sort key `ORDER BY`.

Take the Duplicate Key table as an example. The sort key is defined as `uid` and `name` using `ORDER BY`.

```sql
CREATE TABLE user_access (
    uid int,
    name varchar(64),
    age int, 
    phone varchar(16),
    last_access datetime,
    credits double
)
ORDER BY (uid, name);
```

:::tip

After table creation, you can use `SHOW CREATE TABLE <table_name>;` to view the specified sort columns and the order of these columns in the `ORDER BY` clause from the returned result.
:::

Since the maximum length of a Prefix index entry is 36 bytes, the exceeded part will be truncated. Therefore, each entry in the Prefix index of this table is uid (4 bytes) + name (only the first 32 bytes are taken), and the prefix fields are `uid` and `name`.

**Note**

- The number of prefix fields cannot exceed 3, and the maximum length of a Prefix index entry is 36 bytes.

- Within the prefix fields, columns of the CHAR, VARCHAR, or STRING type can only appear once and must be at the end.

  Take the following table as example, where the first three columns are sort key columns. The prefix field of this table is `name` (20 bytes). It is because that this Prefix index begins with the VARCHAR-type column (`name`) and is truncated directly without including further columns even though the Prefix index entry does not reach 36 bytes in length. Therefore, this Prefix index only contains the `name` field.

    ```SQL
    MySQL [example_db]> describe user_access2;
    +-------------+-------------+------+-------+---------+-------+
    | Field       | Type        | Null | Key   | Default | Extra |
    +-------------+-------------+------+-------+---------+-------+
    | name        | varchar(20) | YES  | true  | NULL    |       |
    | uid         | int         | YES  | true  | NULL    |       |
    | last_access | datetime    | YES  | true  | NULL    |       |
    | age         | int         | YES  | false | NULL    |       |
    | phone       | varchar(16) | YES  | false | NULL    |       |
    | credits     | double      | YES  | false | NULL    |       |
    +-------------+-------------+------+-------+---------+-------+
    6 rows in set (0.00 sec)
    ```

- If the sort key is specified in the table using `ORDER BY`, the Prefix index is formed based on the sort key. If sort key is not specified using `ORDER BY`, the Prefix index is formed based on the Key columns.

### How to design the sort key appropriately to form the Prefix index that can accelerate queries

An analysis of queries and data in business scenarios helps choose appropriate sort key columns and arrange them in a proper order to form a Prefix index, which can significantly improve query performance.

- The number of sort key columns is generally 3 and is not recommended to exceed 4. A sort key with too many columns can not improve query performance but increase the sorting overhead during data loading.
- It is recommended to prioritize columns to form the sort key in the order below:
  1. **Select columns that are frequently used in the query filter conditions as sort key columns.** If the number of the sort key columns is more than one, arrange them in descending order of their frequencies in the query filter conditions. This way, if the query filter conditions include the prefix of the Prefix index, the query performance can be significantly improved. And if the filter conditions include the entire prefix of the Prefix index, the query can fully leverage the Prefix index. Of course, as long as the filter conditions include the prefix, though not the entire prefix, the Prefix index can still optimize the query. However, the effect of the Prefix index will be weakened if the length of the prefix included in the filter conditions is too short. Still, take the [Unique Key table](#usage-notes) whose sort key is `(uid,name)` as an example. If the query filter conditions include the entire prefix, such as `select sum(credits) from user_access where uid = 123 and name = 'Jane Smith';`, the query can fully utilize the Prefix index to improve performance. If the query conditions only include part of the prefix, such as `select sum(credits) from user_access where uid = 123;`, the query can also benefit from the Prefix index to improve performance. However, if the query conditions do not include the prefix, for example, `select sum(credits) from user_access where name = 'Jane Smith';`, the query can not use the Prefix index to accelerate.
  - If multiple sort key columns have similar frequencies as query filter conditions, you can measure the cardinality of these columns.
  - If the cardinality of the column is high, it can filter more data during the query. If the cardinality is too low, such as for Boolean-type columns, its filtering effect is not ideal.

    :::tip
    However, considering the query characteristics in actual business scenarios, usually, slightly lower cardinality columns are more frequently used as query conditions than high cardinality columns. This is because queries whose filtering is frequently based on high cardinality columns, or even in some extreme scenarios, based on columns with UNIQUE constraints are more like point queries in OLTP databases rather than complex analytical queries in OLAP databases.
    :::

  - Also, consider storage compression factors. If the difference in query performance between the order of a low-cardinality column and a high-cardinality one is not readily apparent, placing the low-cardinality column before the high-cardinality one will result in a much higher storage compression rate for the sorted low cardinality column. Therefore, it is recommended to place the low cardinality column in front.

### Considerations for defining sort key columns at table creation

- Data types of sort columns:

  - The sort columns in Primary Key tables support numeric types (including integers, booleans), strings, and date/datetime types.
  - The sort columns in Duplicate Key tables, Aggregate tables, and Unique Key tables support numeric types (including integers, booleans, decimals), strings, and date/datetime types.

- In Aggregate tables and Unique Key tables, sort columns must be defined before other columns.

### Can the Prefix index be modified?

If the characteristics of queries in the business scenario evolve and columns other than the prefix fields are frequently used in the query filter conditions, the existing Prefix index can not filter data and the query performance may not be ideal.

Since v3.0, the sort keys of Primary Key tables can be modified. And since v3.3, the sort keys of Duplicate Key tables, Aggregate tables, and Unique Key tables can be modified. The sort keys in Duplicate Key tables and Primary Key tables can be combination of any sort columns. The sort keys in Aggregate tables and Unique Key tables must include all key columns, but the order of these columns does not need to be consistent with the key columns.

Alternatively, you can also create [synchronous materialized views](../../using_starrocks/Materialized_view-single_table.md) based on this table and choose other columns commonly used as conditional columns to form the Prefix index, which can improve performance for these queries. But note that it will increase storage space.

## How to verify whether the Prefix index accelerates queries

After executing a query, you can check whether the Prefix index takes effect and view its filtering effect from detailed metrics, such as `ShortKeyFilterRows`, in the scan node in the [Query Profile](../../administration/query_profile_overview.md).
