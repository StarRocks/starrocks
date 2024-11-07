---
displayed_sidebar: docs
---

# Sink data from RisingWave to StarRocks

RisingWave is a distributed SQL streaming database that enables simple, efficient, and reliable processing of streaming data. To quickly get started with RisingWave, see [Get started](https://docs.risingwave.com/docs/current/get-started/).

RisingWave provides the data sinking feature to enable users to directly sink data to StarRocks without requiring any other third-party components. This feature can work with all StarRocks table types: Duplicate Key, Primary Key, Aggregate, and Unique Key tables.

## Prerequisites

- You have a running RisingWave cluster of v1.7 or later.
- You can access the target StarRocks table and the StarRocks version is v2.5 or later.
- To sink data into a StarRocks table, you must have the SELECT and INSERT privileges on the target table. To grant the privileges, see [GRANT](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/account-management/GRANT/).

:::tip

RisingWave only supports at-least-once semantics for StarRocks Sink, which means that in case of failures, duplicate data may be written. You are recommended to use [StarRocks Primary Key tables](https://docs.starrocks.io/zh/docs/table_design/table_types/primary_key_table/), which can deduplicate data and achieve end-to-end idempotent writes.

:::

## Parameters

The following table describes the parameters you need to configure when you sink data from RisingWave to StarRocks. All parameters are required unless otherwise specified.

| Parameters                                                       | Description                                                  |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| connector                                                    | Set it to `starrocks`.                                      |
| starrocks.host                                               | The IP address of the StarRocks FE node.      |
| starrocks.query_port                                         | The query port the FE node.          |
| starrocks.http_port                                          | The HTTP port of the FE node.                            |
| starrocks.user                                               | The username used to access the StarRocks cluster.  |
| starrocks.password                                           | The password associated with the username.        |
| starrocks.database                                           | The StarRocks database where the target table is located.                 |
| starrocks.table                                              | The StarRocks table to which you want to sink data.                           |
| starrocks.partial_update                                     | (Optional) Whether to enable the StarRocks partial update feature. Enabling this feature can increase the Sink performance when only a few columns need to be updated.  |
| type                                                         | The data operation type during sink.<ul><li>`append-only`: Performs only INSERT operations. </li><li>`upsert`: Performs Upsert operations. If this setting is used, the StarRocks target table must be a Primary Key table. </li></ul>            |
| force_append_only                                            | (Optional) When `type` is set to `append-only` but there are also Upsert and Delete operations in the sink process, this setting can force the Sink task to generate append-only data and discard Upsert and Delete data. |
| primary_key                                                  | (Optional) The primary key of the StarRocks table. Required if `type` is `upsert`.  |

## Data type mapping

The following table provides the data type mapping between RisingWave and StarRocks.

| RisingWave                                            | StarRocks|
| ----------------------------------------------------- | -------------- |
| BOOLEAN                                               | BOOLEAN        |
| SMALLINT                                              | SMALLINT       |
| INTEGER                                               | INT            |
| BIGINT                                                | BIGINT         |
| REAL                                                  | FLOAT          |
| DOUBLE                                                | DOUBLE         |
| DECIMAL                                               | DECIMAL        |
| DATE                                                  | DATE           |
| VARCHAR                                               | VARCHAR        |
| TIME <br />(Cast to VARCHAR before sinking to StarRocks) | Not supported  |
| TIMESTAMP                                             | DATETIME       |
| TIMESTAMP WITH TIME ZONE <br />(Cast to TIMESTAMP before sinking to StarRocks)  | Not supported  |
| INTERVAL <br />(Cast to VARCHAR before sinking to StarRocks) | Not supported  |
| STRUCT                                                | JSON           |
| ARRAY                                                 | ARRAY          |
| BYTEA <br />(Cast to VARCHAR before sinking to StarRocks)   | Not supported  |
| JSONB                                                 | JSON           |
| SERIAL                                                | BIGINT         |

## Examples

1. Create a database `demo` in StarRocks and create a Primary Key table `score_board` in this database.

   ```sql
   CREATE DATABASE demo;
   USE demo;

   CREATE TABLE demo.score_board(
       id int(11) NOT NULL COMMENT "",
       name varchar(65533) NULL DEFAULT "" COMMENT "",
       score int(11) NOT NULL DEFAULT "0" COMMENT ""
   )
   PRIMARY KEY(id)
   DISTRIBUTED BY HASH(id);
   ```

2. Sink data from RisingWave to StarRocks.

   ```sql
   -- Create a table in RisingWave.
   CREATE TABLE score_board (
       id INT PRIMARY KEY,
       name VARCHAR,
       score INT
   );
   
   -- Insert data into the table.
   INSERT INTO score_board VALUES (1, 'starrocks', 100), (2, 'risingwave', 100);

   -- Sink data from this table to the StarRocks table.
   CREATE SINK score_board_sink
   FROM score_board WITH (
       connector = 'starrocks',
       type = 'upsert',
       starrocks.host = 'starrocks-fe',
       starrocks.mysqlport = '9030',
       starrocks.httpport = '8030',
       starrocks.user = 'users',
       starrocks.password = '123456',
       starrocks.database = 'demo',
       starrocks.table = 'score_board',
         primary_key = 'id'
   );
   ```
