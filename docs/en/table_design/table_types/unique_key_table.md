---
displayed_sidebar: docs
sidebar_position: 50
---

# Unique Key table

<<<<<<< HEAD
When you create a table, you can define primary key columns and metric columns. This way, queries return the most recent record among a group of records that have the same primary key. Compared with the Duplicate Key table, the Unique Key table simplifies the data loading process to better support real-time and frequent data updates.
=======
You need to define a unique key at table creation. When multiple rows of data have the same unique key, the values in the value columns are replaced. During queries, the latest data from a group of data with the same unique key is returned. Additionally, you can define the sort key separately. If the filter conditions in queries include the sort key, StarRocks can quickly filter the data, improving query efficiency.

The Unique Key table can support real-time and frequent data updates. However, it is gradually replaced by the [Primary Key table](./primary_key_table.md).
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## Scenarios

The Unique Key table is suitable for business scenarios in which data needs to be frequently updated in real time. For example, in e-commerce scenarios, hundreds of millions of orders can be placed per day, and the statuses of the orders frequently change.

## Principle

<<<<<<< HEAD
The Unique Key table can be considered a special Aggregate Key table in which the REPLACE aggregate function is specified for metric columns to return the most recent record among a group of records that have the same primary key.

When you load data into a table that uses the Unique Key table, the data is split into multiple batches. Each batch is assigned a version number. Therefore, records with the same primary key may come in multiple versions, of which the most recent version (namely, the record with the largest version number) is retrieved for queries.

As shown in the following table, `ID` is the primary key column, `value` is a metric column, and `_version` holds the data version numbers generated within StarRocks. In this example, the record with an `ID` of 1 is loaded by two batches whose version numbers are `1` and `2`, and the record with an `ID` of `2` is loaded by three batches whose version numbers are `3`, `4`, and `5`.
=======
The Unique Key table can be considered a special Aggregate table in which the REPLACE aggregate function is specified for value columns to return the most recent record among a group of records that have the same unique key.

When you load data into a Unique Key table, the data is split into multiple batches. Each batch is assigned a version number. Therefore, records with the same unique key may be included in multiple versions. Data in the most recent version (that is, the record with the largest version number) is returned for queries.

As shown in the following table, `ID` is the unique key, `value` is a value column, and `_version` holds the data version numbers generated within StarRocks. In this example, the record with an `ID` of 1 is loaded by two batches whose version numbers are `1` and `2`, and the record with an `ID` of `2` is loaded by three batches whose version numbers are `3`, `4`, and `5`.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

| ID   | value | _version |
| ---- | ----- | -------- |
| 1    | 100   | 1        |
| 1    | 101   | 2        |
| 2    | 100   | 3        |
| 2    | 101   | 4        |
| 2    | 102   | 5        |

When you query the record with an `ID` of `1`, the most recent record with the largest version number, which is `2` in this case, is returned. When you query the record with an `ID` of `2`, the most recent record with the largest version number, which is `5` in this case, is returned. The following table shows the records returned by the two queries:

| ID   | value |
| ---- | ----- |
| 1    | 101   |
| 2    | 102   |

## Create a table

<<<<<<< HEAD
In e-commerce scenarios, you often need to collect and analyze the statuses of orders by date. In this example, create a table named `orders` to hold the orders, define `create_time` and `order_id`, which are frequently used as conditions to filter the orders, as primary key columns, and define the other two columns, `order_state` and `total_price`, as metric columns. This way, the orders can be updated in real time as their statuses change, and can be quickly filtered to accelerate queries.
=======
In e-commerce scenarios, you often need to collect and analyze the statuses of orders by date. In this example, create a table named `orders` to hold the orders, define `create_time` and `order_id`, which are frequently used as conditions to filter the orders, as unique key columns, and define the other two columns, `order_state` and `total_price`, as value columns. This way, the orders can be updated in real time as their statuses change, and can be quickly filtered to accelerate queries.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

The statement for creating the table is as follows:

```SQL
<<<<<<< HEAD
CREATE TABLE IF NOT EXISTS orders (
=======
CREATE TABLE orders (
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
    create_time DATE NOT NULL COMMENT "create time of an order",
    order_id BIGINT NOT NULL COMMENT "id of an order",
    order_state INT COMMENT "state of an order",
    total_price BIGINT COMMENT "price of an order"
)
UNIQUE KEY(create_time, order_id)
DISTRIBUTED BY HASH(order_id);
```

> **NOTICE**
>
> - When you create a table, you must specify the bucketing column by using the `DISTRIBUTED BY HASH` clause. For detailed information, see [bucketing](../data_distribution/Data_distribution.md#bucketing).
<<<<<<< HEAD
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [determine the number of buckets](../data_distribution/Data_distribution.md#determine-the-number-of-buckets).

## Usage notes

- Take note of the following points about the primary key of a table:

  - The primary key is defined by using the `UNIQUE KEY` keyword.
  - The primary key must be created on columns on which unique constraints are enforced and whose names cannot be changed.
  - The primary key must be properly designed:
    - When queries are run, primary key columns are filtered before the aggregation of multiple data versions, whereas metric columns are filtered after the aggregation of multiple data versions. Therefore, we recommend that you identify the columns that are frequently used as filter conditions and define these columns as primary key columns. This way, data filtering can start before the aggregation of multiple data versions to improve query performance.
    - During the aggregation process, StarRocks compares all primary key columns. This is time-consuming and may decrease query performance. Therefore, do not define a large number of primary key columns. If a column is rarely used as a filter condition for queries, we recommend that you do not define the column as a primary key column.

- When you create a table, you cannot create BITMAP indexes or Bloom Filter indexes on the metric columns of the table.

- The Unique Key table does not support materialized views.
=======
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../data_distribution/Data_distribution.md#set-the-number-of-buckets).

## Usage notes

- **Unique Key**:
  - In the CREATE TABLE statement, the unique key must be defined before other columns.
  - The unique key needs to be explicitly defined using `UNIQUE KEY`.
  - The unique key has uniqueness constraint.

- **Sort Key**:

  - Since v3.3.0, the sort key is decoupled from the unique key in the Unique Key table. The Unique Key table supports specifying the sort key using `ORDER BY` and specifying the unique key using `UNIQUE KEY`. The columns in the sort key and the unique key need to be the same, but the order of the columns does not need to be the same.

  - During queries, data can be filtered based on the sort keys before aggregation. However, data can be filtered based on the value columns after multi-version aggregation. Therefore, it is recommended to use frequently filtered fields as sort keys to filter data before aggregation and thereby improve query performance.

- When you create a table, you can only create Bitmap indexes or Bloom Filter indexes on the key columns of the table.
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Loading options](../../loading/Loading_intro.md).

<<<<<<< HEAD
> - When you load data into a table that uses the Unique Key table, you can only update all columns of the table. For example, when you update the preceding `orders` table, you must update all its columns, which are `create_time`, `order_id`, `order_state`, and `total_price`.
> - When you query data from a table that uses the Unique Key table, StarRocks needs to aggregate records of multiple data versions. In this situation, a large number of data versions decreases query performance. Therefore, we recommend that you specify a proper frequency at which data is loaded into the table to meet meet your requirements for real-time data analytics while preventing a large number of data versions. If you require minute-level data, you can specify a loading frequency of 1 minute instead of a loading frequency of 1 second.
=======
:::note

- When you load data into a table that uses the Unique Key table, you can only update all columns of the table. For example, when you update the preceding `orders` table, you must update all its columns, which are `create_time`, `order_id`, `order_state`, and `total_price`.
- When you query data from a table that uses the Unique Key table, StarRocks needs to aggregate records of multiple data versions. In this situation, a large number of data versions decreases query performance. Therefore, we recommend that you specify a proper frequency at which data is loaded into the table to meet meet your requirements for real-time data analytics while preventing a large number of data versions. If you require minute-level data, you can specify a loading frequency of 1 minute instead of a loading frequency of 1 second.

:::
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
