---
displayed_sidebar: docs
---

# Unique Key table

You need to define a unique key at table creation. When multiple rows of data have the same unique key, the values in the value columns are replaced. During queries, the latest data from a group of data with the same unique key is returned. Additionally, you can define the sort key separately. If the filter conditions in queries include the sort key, StarRocks can quickly filter the data, improving query efficiency.

The Unique Key table can support real-time and frequent data updates. However, it is gradually replaced by the [Primary Key table](./primary_key_table.md).

## Scenarios

The Unique Key table is suitable for business scenarios in which data needs to be frequently updated in real time. For example, in e-commerce scenarios, hundreds of millions of orders can be placed per day, and the statuses of the orders frequently change.

## Principle

The Unique Key table can be considered a special Aggregate table in which the REPLACE aggregate function is specified for value columns to return the most recent record among a group of records that have the same unique key.

When you load data into a Unique Key table, the data is split into multiple batches. Each batch is assigned a version number. Therefore, records with the same unique key may be included in multiple versions. Data in the most recent version (that is, the record with the largest version number) is returned for queries.

As shown in the following table, `ID` is the unique key, `value` is a value column, and `_version` holds the data version numbers generated within StarRocks. In this example, the record with an `ID` of 1 is loaded by two batches whose version numbers are `1` and `2`, and the record with an `ID` of `2` is loaded by three batches whose version numbers are `3`, `4`, and `5`.

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

In e-commerce scenarios, you often need to collect and analyze the statuses of orders by date. In this example, create a table named `orders` to hold the orders, define `create_time` and `order_id`, which are frequently used as conditions to filter the orders, as unique key columns, and define the other two columns, `order_state` and `total_price`, as value columns. This way, the orders can be updated in real time as their statuses change, and can be quickly filtered to accelerate queries.

The statement for creating the table is as follows:

```SQL
CREATE TABLE orders (
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
> - When you create a table, you must specify the bucketing column by using the `DISTRIBUTED BY HASH` clause. For detailed information, see [bucketing](../Data_distribution.md#bucketing).
> - Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../Data_distribution.md#set-the-number-of-buckets).

## Usage notes

- **Unique Key**:
  - In the CREATE TABLE statement, the unique key must be defined before other columns.
  - The unique key needs to be explicitly defined using `UNIQUE KEY`.
  - The unique key has uniqueness constraint.

- **Sort Key**:

  - Since v3.3.0, the sort key is decoupled from the unique key in the Unique Key table. The Unique Key table supports specifying the sort key using `ORDER BY` and specifying the unique key using `UNIQUE KEY`. The columns in the sort key and the unique key need to be the same, but the order of the columns does not need to be the same.

  - During queries, data can be filtered based on the sort keys before aggregation. However, data can be filtered based on the value columns after multi-version aggregation. Therefore, it is recommended to use frequently filtered fields as sort keys to filter data before aggregation and thereby improve query performance.

- When you create a table, you can only create Bitmap indexes or Bloom Filter indexes on the key columns of the table.

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Loading options](../../loading/loading_introduction/Loading_intro.md).

:::note

- When you load data into a table that uses the Unique Key table, you can only update all columns of the table. For example, when you update the preceding `orders` table, you must update all its columns, which are `create_time`, `order_id`, `order_state`, and `total_price`.
- When you query data from a table that uses the Unique Key table, StarRocks needs to aggregate records of multiple data versions. In this situation, a large number of data versions decreases query performance. Therefore, we recommend that you specify a proper frequency at which data is loaded into the table to meet meet your requirements for real-time data analytics while preventing a large number of data versions. If you require minute-level data, you can specify a loading frequency of 1 minute instead of a loading frequency of 1 second.

:::
