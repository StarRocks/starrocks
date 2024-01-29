---
displayed_sidebar: "English"
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

For a Duplicate Key table, sort key columns are specified in `DUPLICATE KEY`. For an Aggregate table or a Unique Key table, sort key columns are coupled with constrainted columns, and are specified in `AGGREGATE KEY` or `UNIQUE KEY`. From v3.0 onwards, a Primary Key table decouples sort key columns and primary key columns, providing more flexible capabilities. The sort key columns are specified in `ORDER BY`, and primary key columns are specified in `PRIMARY KEY`.

The following example uses a Unique Key table to illustrate how to specify sort key columns at table creation and how they comprise a Prefix index.

When creating the Unique Key table, you can specify sort key columns as `uid` and `name` in `DUPLICATE KEY`.

```SQL
CREATE TABLE user_access (
    uid int,
    name varchar(64),
    age int, 
    phone varchar(16),
    last_access datetime,
    credits double
)
DUPLICATE KEY(uid, name);
```

:::note

After creating a Unique Key table, Aggregate table, or Unique Key table, you can use `DESCRIBE <table_name>;` to view its sort key columns. In the returned result, the columns whose `Key` field shows `true` are sort key columns. After creating a Primary Key table, you can use `SHOW CREATE TABLE <table_name>;` to view its sort key columns. In the returned result, the columns in `ORDER BY` clause are sort key columns.

:::

Since the maximum length of a Prefix index entry is 36 bytes, the exceeded part will be truncated. Therefore, each entry in the Prefix index of this table is uid (4 bytes) + name (only the first 32 bytes are taken), and the prefix fields are `uid` and `name`.

**Precaution**

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

### How to design the sort key appropriately to form the Prefix index that can accelerate queries

An analysis of queries and data in business scenarios helps choose appropriate sort key columns and arrange them in a proper order to form a Prefix index, which can significantly improve query performance.

:::tip

Except for Primary Key tables, currently the sort key for other types of tables **cannot be modified** after table creation. Therefore, it is recommended to carefully analyze the characteristics of data and queries to design a suitable sort key.

:::

- The number of sort key columns is generally 3 and is not recommended to exceed 4. A sort key with too many columns can not improve query performance but increase the sorting overhead during data loading.
- It is recommended to prioritize columns to form the sort key in the order below:
  1. **Select columns that are frequently used in query filter conditions as sort key columns.** If the number of the sort key columns is more than one, arrange them in descending order of their frequencies in query filter conditions. This way, if the query filter conditions include the prefix of the Prefix index, the query performance can be significantly improved. And if the filter conditions include the entire prefix of the Prefix index, the query can fully leverage the Prefix index. Of course, as long as the filter conditions include the prefix, though not the entire prefix, the Prefix index can still optimize the query. However, the effect of the Prefix index will be weakened if the length of the prefix included in the filter conditions is too short. Still, take the [Unique Key table](https://chat.openai.com/c/0c47f67a-8103-4ec6-a280-71495f037334#Usage-Guidelines) whose sort key is `(uid,name)` as an example. If the query filter conditions include the entire prefix, such as `select sum(credits) from user_access where uid = 123 and name = 'Jane Smith';`, the query can fully utilize the Prefix index to improve performance. If the query conditions only include part of the prefix, such as `select sum(credits) from user_access where uid = 123;`, the query can also benefit from the Prefix index to improve performance. However, if the query conditions do not include the prefix, for example, `select sum(credits) from user_access where name = 'Jane Smith';`, the query can not use the Prefix index to accelerate.
  - If multiple sort key columns have similar frequencies as query filter conditions, you can measure the cardinality of these columns.
  - If the cardinality of the column is high, it can filter more data during the query. If the cardinality is too low, such as for Boolean-type columns, its filtering effect is not ideal. 
    :::tip
    
    However, considering the query characteristics in actual business scenarios, usually, slightly lower cardinality columns are more frequently used as query conditions than high cardinality columns. This is because queries whose filtering is frequently based on high cardinality columns, or even in some extreme scenarios, based on columns with UNIQUE constraints are more like point queries in OLTP databases rather than complex analytical queries in OLAP databases.
    
    :::
  - Also, consider storage compression factors. If the difference in query performance between the order of a low-cardinality column and a high-cardinality one is not readily apparent, placing the low-cardinality column before the high-cardinality one will result in a much higher storage compression rate for the sorted low cardinality column. Therefore, it is recommended to place the low cardinality column in front.

### Considerations for defining sort key columns at table creation

When defining sort key columns for a Duplicate Key table, an Aggregate table, or a Unique Key table, pay attention to the following points:

- The sort key columns must be defined before other columns at table creation.
- The sort key columns must be the first one or more columns in the table, and the order of the sort key columns must match the sequence of these columns in the table.
- The data types of the sort key columns can be numeric types (excluding DOUBLE and FLOAT), strings, and date types.

### Can the Prefix index be modified?

The Prefix indexes can not be modified directly after table creation (except for Primary Key tables) because the sort key columns can not be changed. If columns other than the prefix fields are frequently used in query filter conditions, the existing Prefix index can not filter data and the query performance may not be ideal. Then you can also create a [synchronous materialized view](../../using_starrocks/Materialized_view-single_table.md) based on this table and choose other columns commonly used as conditional columns to form the Prefix index, which can improve performance for these queries. But note that it will increase storage space.
