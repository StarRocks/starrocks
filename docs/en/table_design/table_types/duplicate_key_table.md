---
displayed_sidebar: docs
---

# Duplicate Key table

The Duplicate Key table is the default model in StarRocks. If you did not specify a model when you create a table, a Duplicate Key table is created by default.

When you create a Duplicate Key table, you can define a sort key for that table. If the filter conditions contain the sort key columns, StarRocks can quickly filter data from the table to accelerate queries.

The Duplicate Key table is suitable for scenarios, such as analyzing logs data. It supports appending new data, but does not support modifying historical data.

## Scenarios

The Duplicate Key table is suitable for the following scenarios:

- Analyze raw data, such as raw logs and raw operation records.
- Query data by using a variety of methods without being limited by the pre-aggregation method.
- Load log data or time-series data. New data is written in append-only mode, and existing data is not updated.

## Create a table

Suppose that you want to analyze the event data over a specific time range. In this example, create a table named `detail` and define `event_time` and `event_type` as sort key columns.

Statement for creating the table:

```SQL
CREATE TABLE detail (
    event_time DATETIME NOT NULL COMMENT "datetime of event",
    event_type INT NOT NULL COMMENT "type of event",
    user_id INT COMMENT "id of user",
    device_code INT COMMENT "device code",
    channel INT COMMENT "")
ORDER BY (event_time, event_type);
```

## Usage notes

- **Sort key**

  Since v3.3.0, the Duplicate Key table supports specifying the sort key using `ORDER BY`, which can be combination of any columns. If both `ORDER BY` and `DUPLICATE KEY` are used, `DUPLICATE KEY` does not take effect. If neither `ORDER BY` nor `DUPLICATE KEY` is used, the first three columns of the table are used as the sort key by default.

- **Bucketing**

  - **Bucketing method**:

  Since v3.1.0, StarRocks supports random bucketing for Duplicate Key tables (the default bucketing method). When creating tables or adding partitions, you do not need to set the hash bucketing key (that is the `DISTRIBUTED BY HASH` clause). Before v3.1.0, StarRocks only supports hash bucketing. You need to set the hash bucketing key (that is the `DISTRIBUTED BY HASH` clause) when creating tables or adding partitions, otherwise the table fails to be created. For more information on hash bucketing keys, see [Hash Bucketing](../Data_distribution.md#hash-bucketing).

  - **Bucket number**: Since v2.5.7, StarRocks can automatically set the number of buckets (BUCKETS) when you create a table or add a partition. You no longer need to manually set the number of buckets. For detailed information, see [set the number of buckets](../Data_distribution.md#set-the-number-of-buckets).

- When you create a table, you can create Bitmap indexes or Bloom Filter indexes on the all columns of the table.

## What to do next

After a table is created, you can use various data ingestion methods to load data into StarRocks. For information about the data ingestion methods that are supported by StarRocks, see [Loading options](../../loading/loading_introduction/Loading_intro.md).

:::note

- When you load data into a table that uses the Duplicate Key table, you can only append data to the table. You cannot modify the existing data in the table.
- If two identical records are loaded, the Duplicate Key table retains them as two records, rather than one.

:::
