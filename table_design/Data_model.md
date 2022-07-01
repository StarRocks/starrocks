# Data models

StarRocks provides the following four data models based on the mapping between ingested data and actually stored data: Duplicate Key, Aggregate Key, Unique Key, and Primary Key. StarRocks uses the dimension columns of a table as sort keys for that table. Sort keys in StarRocks have the following advantages over traditional primary keys:

- All sort key columns and primary key columns are dimension columns.
- Sort keys do not need to be unique. Duplicate sort keys are allowed.
- The columns of a table are clustered and stored in sort key order.
- Sort keys support shortkey indexes.

When StarRocks populates a table with multiple ingested rows whose primary keys are duplicate, it processes the table in one of the following ways depending on which data model is used by the table:

- Duplicate Key

If StarRocks can find the same rows in the table, it maps the ingested rows to the found rows in one-to-one relationships. You can retrieve all ingested data.

- Aggregate Key

  If StarRocks cannot find the same rows in the table, it invokes an aggregate function to merge the metric columns of the ingested rows into one row. You can retrieve the accumulative results of all ingested data. However, you cannot retrieve all ingested data.

- Unique Key or Primary Key

  The table has a unique primary key. StarRocks replaces the rows whose primary keys are duplicate with the ingested rows. The result is similar to a group of most recent data returned by the REPLACE aggregate function when you call the function on a table that uses the Aggregate Key model.

Take note of the following points:

- Sort key columns must be defined prior to metric columns in a table creation statement.

- The order of sort key columns in a table creation statement specifies the order of the conditions based on which the rows in the table are sorted.

- Shortkey indexes on sort keys are created on columns whose names are prefixed.

This topic describes the characteristics and scenarios of the data models to help you choose a data model that is well suited to your business scenario.

## Duplicate Key model

### Scenarios

The Duplicate Key model is the default data model for tables in StarRocks.

The Duplicate Key model is suitable for scenarios that have the following characteristics:

- Raw data, such as raw logs and raw operation records, must be retained for analysis.
- Query methods are flexible. You do not need to predefine a query method. Additionally, if traditional preaggregate methods are used, the data requested by queries is difficult to fetch.
- Most of the source data is log data or time series data, which is written in append-only mode. The data is infrequently updated and does not change much after it is ingested.

### How it works

You can specify sort key columns for a table. If you do not explicitly specify sort key columns for a table, StarRocks selects a few default columns as sort key columns for the table. This way, StarRocks can quickly filter data to reduce query latencies if the sort key columns can be used as filter conditions.

> Note: If you load two identical rows into a table that uses the Duplicate Key model, StarRocks considers the two identical rows to be two distinct rows.

### How to use it

By default, each table that you create in StarRocks uses the Duplicate Key model. Sort key columns support shortkey indexes, based on which data can be quickly filtered. You can place the definitions of the dimension columns that are frequently used as filter conditions prior to the definitions of the other columns. For example, if a specific type of event over a specific time range is frequently queried, you can specify event time and event type as sort keys.

The following example shows how to create a table that uses the Duplicate Key model. In the table creation statement, `DUPLICATE KEY(event_time, event_type)` specifies that the Duplicate Key model is used and specifies the sort keys that you want to use. Note that the definitions of sort key columns are prior to the definitions of the other columns.

```SQL
CREATE TABLE IF NOT EXISTS detail (

    event_time DATETIME NOT NULL COMMENT "datetime of event",

    event_type INT NOT NULL COMMENT "type of event",

    user_id INT COMMENT "id of user",

    device_code INT COMMENT "device of ",

    channel INT COMMENT ""

)

DUPLICATE KEY(event_time, event_type)

DISTRIBUTED BY HASH(user_id) BUCKETS 8
```

### Usage notes

- Fully utilize sort key columns. When you create a table, place the definitions of the columns that are frequently used as filter conditions prior to the other columns. This approach helps accelerate queries.
- The Duplicate Key model allows you to define a few columns as sort keys. In the Aggregate Key model and Unique Key model, all dimension columns are used as sort keys.

## Aggregate Key model

### Scenarios

The Aggregate Key model helps you collect and aggregate data in various data analytics scenarios. A few examples are as follows:

- Help website or app providers analyze the amount of traffic and time that their users spend on a specific website or app and the total number of visits to the website or app.
- Help advertising agencies analyze the total clicks, total views, and consumption statistics of an advertisement that they provide for their customers.
- Help e-commerce companies analyze their annual trading data to identify the geographic bestsellers within individual quarters or months.

The Aggregate Key model is suitable for scenarios that have the following characteristics:

- Aggregate queries, such as SUM, COUNT, and MAX, are run.
- Raw detailed data does not need to be retrieved.
- Old data is not frequently updated but is appended with new data.

### How it works

StarRocks aggregates the metric columns of a table based on the dimension columns of the table. If multiple data records are available in the same dimension, StarRocks aggregates the data records to reduce the amount of data that needs to be processed. This increases query efficiency.

Suppose that you have a table that uses the Aggregate Key model and the table consists of four raw data records.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 1    |
| 2020.05.01 | CHN     | 2    |
| 2020.05.01 | USA     | 3    |
| 2020.05.01 | USA     | 4    |

StarRocks aggregates the four raw data records into two data records.

| Date       | Country | PV   |
| ---------- | ------- | ---- |
| 2020.05.01 | CHN     | 3    |
| 2020.05.01 | USA     | 7    |

### How to use it

To enable the Aggregate Key model for a table, you only need to specify an aggregate function in the definitions of metric columns at table creation. You can use the `AGGREGATE KEY` keyword to explicitly define sort keys.

The following code snippet provides an example on how to create a table that uses the Aggregate Key model:

- The `site_id`, `date`, and `city_code` columns are defined as sort keys.

- The `pv` column is a metric column on which the aggregate function SUM is specified.

```SQL
CREATE TABLE IF NOT EXISTS example_db.aggregate_tbl (

    site_id LARGEINT NOT NULL COMMENT "id of site",

    date DATE NOT NULL COMMENT "time of event",

    city_code VARCHAR(20) COMMENT "city_code of user",

    pv BIGINT SUM DEFAULT "0" COMMENT "total page views"

)

DISTRIBUTED BY HASH(site_id) BUCKETS 8;
```

### Usage notes

- StarRocks batch loads the data of a table that uses the Aggregate Key model and generates a data version for each load job. StarRocks triggers an aggregation of rows that have the same sort key in one of the following ways:
  - Aggregates the data at data load before the data is flushed to the disk.
  - Asynchronously aggregates multiple data versions in the background after the data is flushed to the disk.
  - Aggregates multiple data versions from multiple pipelines at data query.

- When a query is run, StarRocks aggregates and then filters metric columns to identify the columns that do not need to be defined as metric columns. Then, StarRocks stores the identified columns as dimension columns.
- For information about the aggregate functions that are supported by the Aggregate Key model, see [CREATE TABLE](/sql-reference/sql-statements/data-definition/CREATE%20TABLE.md).

## Unique Key model

### Scenarios

StarRocks provides the Unique Key model to meet the requirements for data updates in specific data analytics scenarios. For example, in e-commerce scenarios, hundreds of millions of orders can be placed per day, and the statuses of the orders change frequently. If you use the Duplicate Key model, you cannot frequently update such a large volume of data in real time by performing delete and insert operations. We recommend that you choose the Unique Key model in these scenarios. However, if you need to update data in a more frequent and more real-time manner, we recommend that you choose the Primary Key model.

The Unique Key model is suitable for scenarios that have the following characteristics:

- A large amount of existing data needs to be updated.
- Data needs to be analyzed in a real-time manner.

### How it works

In the Unique Key model, each sort key is unique and can be used as a primary key.

StarRocks assigns a version number to each batch of ingested data. Data with the same primary key may be available in multiple versions, of which the most recent version is retrieved for queries.

| ID   | value | _version |
| ---- | ----- | -------- |
| 1    | 100   | 1        |
| 1    | 101   | 2        |
| 2    | 100   | 3        |
| 2    | 101   | 4        |
| 2    | 102   | 5        |

In the preceding example, `ID` is the primary key of the table, the `value` column holds the data of the table, and the `_version` column holds the version number of each batch of ingested data. In this example, the row with an ID of 1 is loaded in two batches, numbered 1 and 2, and the row with an ID of 2 is loaded in three batches, numbered 3, 4, and 5. If you query the row with an ID of 1, the batch numbered 2 is returned. If you query the row with an ID of 2, the batch numbered 5 is returned. The following table lists the data that is returned for these queries.

IDvalue11012102

With the Unique Key model, StarRocks can support analytics of frequently updated data.

### How to use it

In e-commerce scenarios, you need to collect and analyze the statuses of orders that are recorded in a table. The statuses of orders change frequently, whereas the creation time (`create_time`) and IDs (`order_id`) of orders never change. Therefore, you can define the `create_time` and `order_id` columns of the table as the primary key by using the `UNIQUE KEY` keyword at table creation. This way, the statuses of orders can be frequently updated, and the data of the table can be quickly filtered to return query results.

The following example shows how to create a table that uses the Unique Key model:

```SQL
CREATE TABLE IF NOT EXISTS detail (

    create_time DATE NOT NULL COMMENT "create time of an order",

    order_id BIGINT NOT NULL COMMENT "id of an order",

    order_state INT COMMENT "state of an order",

    total_price BIGINT COMMENT "price of an order"

)

UNIQUE KEY(create_time, order_id)

DISTRIBUTED BY HASH(order_id) BUCKETS 8
```

- Use the `UNIQUE KEY` keyword to define the **create_time** and **order_id** columns as the primary key. These columns are also used as sort keys. Make sure that the definitions of these columns are placed prior to the definitions of the other columns of the table.
- The `order_state` and `total_price` columns are metric columns on which the aggregate function REPLACE is defined.

### Usage notes

- When you load data by performing an update operation, you must specify all of the following  fields to ensure that the update operation can be completed: `create_time`, `order_id`, `order_state`, and `total_price`.
- When data is queried from a table that uses the Unique Key model, StarRocks needs to merge multiple data versions. If a large number of data versions are generated from data ingestions, version merging impairs query performance. Therefore, we recommend that you ingest data at a proper loading frequency to ensure query performance while meeting your requirements for real-time data. If you require minute-level real-time data, you can specify a 1-minute loading frequency rather than a second-level loading frequency.
- In most cases, when data is queried from a table that uses the Unique Key model, StarRocks filters the data held in the `value` column after it finishes merging multiple data versions. Identify the columns that are often used as filter conditions and will not be changed, and define the columns as the primary key. This way, StarRocks can filter the data held in the columns out before version merging to improve query performance.
- StarRocks compares all fields that comprise the primary key during version merging. Therefore, do not define a large number of fields as the primary key. If you define a large number of fields as the primary key, query performance deteriorates. As such, if a field is occasionally used as a filter condition, we recommend that you do not define the field as the primary key.

## Primary Key model

### Scenarios

StarRocks has started to support the Primary Key model since v1.19. If a table uses the Primary Key model, a unique primary key must be defined for the table. You can perform update and delete operations on the rows of the table by using the primary key. The Primary Key model is more suitable than the Unique Key model in terms of support for real-time and frequent updates.

The Primary Key model is suitable in the following scenarios:

- **Stream data in real time from transaction processing systems into StarRocks.** In normal cases, transaction processing systems involve a large number of update and delete operations in addition to insert operations. If you need to synchronize data from a transaction processing system to StarRocks, you can create a table that uses the Primary Key model. Then, you can use tools, such as CDC Connectors for Apache Flink®, to synchronize the binary logs of the transaction processing system to StarRocks. StarRocks uses the binary logs to add, delete, and update data in the table in real time. This simplifies data synchronization and delivers 3 to 10 times higher query performance than when a Merge on Read (MoR) table of the Unique Key model is used. For example, you can use flink-connector-starrocks to load data. For more information, see [Load data by using flink-connector-starrocks](/loading/Flink-connector-starrocks.md).

- **Join multiple streams by performing update operations on individual columns**. In business scenarios such as user profiling, flat tables are used to improve multi-dimensional analysis performance and simplify the analytics model that is used by data analysts. Upstream data in these scenarios may come from various apps, such as shopping apps, delivery apps, and banking apps, or systems, such as machine learning systems that perform computations to obtain the distinct tags and properties of users. The Primary Key model is well suited in these scenarios, because it supports updates to individual columns. Each app or system can update only the columns that hold the data within its own service scope while benefiting from real-time data additions, deletions, and updates and high query performance.

Note that the storage engine creates an index for the primary key of each table. When you run a data load job, the primary key index is loaded into the memory. Therefore, the Primary Key model requires a large memory capacity. This makes the Primary Key mode unsuitable for tables whose primary key consists of a large number of columns. **To prevent primary keys from exhausting memory, StarRocks limits the total length of the fields that comprise the primary key of each table to 127 bytes after encoding.** Consider using the Primary Key model if a table has the following characteristics:

- The table contains both fast-changing data and slow-changing data. Fast-changing data is frequently updated over the most recent days, whereas slow-changing data is seldom updated. Suppose that you need to synchronize a MySQL order table to StarRocks in real time for analytics and queries. In this example, the data of the table is partitioned on a daily basis, and most updates are performed on orders that were created within the most recent days. Old orders are no longer updated after they are completed. When you run a data load job, the primary key index only the data in the primary key columns for new orders is loaded into the memory, whereas the data in the primary key columns for old orders is not loaded.

![fig1](/assets/3.2.4-1.png)

The table shown in the preceding figure is partitioned on a daily basis, and the primary key-related data in the most recently created partitions is updated more frequently than the data in the other partitions.

The preceding figure shows that the data related to the primary key.

- Your database system uses flat tables. Each flat table consists hundreds to thousands of columns. Primary key-related data comprises only a small portion of the table data and consumes only a small amount of memory. For example, a user status or profile table consists of a large number of columns but only tens to hundreds of millions of users. In this situation, the amount of memory consumed by the data in primary key columns can be limited.

![fig1](/assets/3.2.4-2.png)

The primary key-related data in the flat table shown in the preceding figure comprises only a small portion of the table, and the table consists a small number of rows.

### How it works

The Primary Key model is supported by a new storage engine that is designed by StarRocks. The metadata structure, reads, and writes in the Primary Key model differ from the Duplicate Key, Aggregate Key, and Unique Key models.

The Duplicate Key, Aggregate Key, and Unique Key models adopt the MoR storage type, which makes data writes efficient. However, these models require online merging of multiple data versions during queries, and the Merge operator prevents the pushdown of predicates and the use of indexes. As a result, query performance deteriorates. In this situation, the Primary Key model can ensure that each primary key maps only to a single data record. By doing so, the Primary Key model prevents merge operations. Details are as follows:

- When StarRocks receives a request for an update operation on a data record, it locates the data record based on the primary key index, marks the data record as deleted, and inserts a new data record. This means that StarRocks converts an update operation to a delete operation plus an insert operation.

- When StarRocks receives a delete operation on a data record, it locates the data record based on the primary key index and marks the data record as deleted. By doing so, StarRocks prevents impacts on the pushdown of predicates and the use of indexes to efficiently run queries.

The Primary Key model gives up a little write performance and consumes a little more memory to deliver significantly higher query performance than the Unique Key model.

### How to use it

#### Create a table

When you create a table that uses the Primary Key model, use the `PRIMARY KEY` keyword to specify the first one or more columns as the primary key of the table. This approach is similar in the other database systems.

Example 1: Create an order table that uses the Primary Key model and is partitioned on a daily basis.

```SQL
create table orders (

    dt date NOT NULL,

    order_id bigint NOT NULL,

    user_id int NOT NULL,

    merchant_id int NOT NULL,

    good_id int NOT NULL,

    good_name string NOT NULL,

    price int NOT NULL,

    cnt int NOT NULL,

    revenue int NOT NULL,

    state tinyint NOT NULL

) PRIMARY KEY (dt, order_id)

PARTITION BY RANGE(`dt`) (

    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),

    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22')),

    ...

    PARTITION p20210929 VALUES [('2021-09-29'), ('2021-09-30')),

    PARTITION p20210930 VALUES [('2021-09-30'), ('2021-10-01'))

) DISTRIBUTED BY HASH(order_id) BUCKETS 4

PROPERTIES("replication_num" = "3",
"replication_num" = "3");
```

Example 2: Create a status table that uses the Primary Key model.

```SQL
create table users (

    user_id bigint NOT NULL,

    name string NOT NULL,

    email string NULL,

    address string NULL,

    age tinyint NULL,

    sex tinyint NULL,

    last_active datetime,

    property0 tinyint NOT NULL,

    property1 tinyint NOT NULL,

    property2 tinyint NOT NULL,

    property3 tinyint NOT NULL,

    ....

) PRIMARY KEY (user_id)

DISTRIBUTED BY HASH(user_id) BUCKETS 4

PROPERTIES("replication_num" = "3",
"replication_num" = "3");
```

Take note of the following points:

- Primary key columns do not allow `NULL` values and support only the following data types: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, STRING (or VARCHAR), DATE, and DATETIME.
- The partition column and bucket column must be primary key columns.
- Unlike the Unique Key model, the Primary Key model allows you to create indexes, such as bitmap indexes, on non-primary key columns. Note that you must create indexes at table creation.
- The Primary Key model does not support rollup indexes and materialized views because the values held in columns may change.
- You cannot change the data type of a column by using the ALTER TABLE statement. For information about the syntax and examples of the ALTER TABLE statement, see [ALTER TABLE](/sql-reference/sql-statements/data-definition/ALTER%20TABLE.md).
- When you design a table, specify as few columns as you can and reduce the table size to save memory resources. We recommend that you use data types, such as INT and BIGINT, that consume less memory than the other data types. We recommend that you do not choose the VARCHAR data type. Before you create a table, we recommend that you check the table design and estimate the memory consumption for the table based on the number of columns and the data types of primary key columns. This way, you can prevent your StarRocks cluster from running out of memory.

  For example, a table consists of 10 million rows and is stored with three replicas, and the primary key of the table is created on the following two columns: `dt date (4byte), id bigint(8byte) = 12byte`. In this case, the estimated amount of memory that is consumed by the table is calculated as follows:

 `(12 + 9 1000W * 3 * 1.5 (average additional overhead per hash table) = 945 MB`

  In the preceding formula, `9` is the fixed memory consumption per row, and `1.5` indicates the average additional memory consumpdtion per hash table.

- `enable_persistent_index`: whether to enable persistent primary key indexes. A persistent primary key index is stored in both the disk and memory to prevent excessive memory consumption. The value can be `true` or `false`. If the disk is SSD, we recommend that you set this parameter to `true`.

#### Update a table

You can run a  stream load, broker load, or routine load job to perform insert, update, or delete operations on all or individual columns of a table that uses the Primary Key model. For more information, see [Load data into tables of Primary Key model](/loading/Load_to_Primary_Key_tables.md).
