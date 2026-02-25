---
displayed_sidebar: docs
---

# Continuously load data from Apache Flink®

StarRocks provides a self-developed connector named StarRocks Connector for Apache Flink® (Flink connector for short) to help you load data into a StarRocks table by using Flink. The basic principle is to accumulate the data and then load it all at a time into StarRocks through [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md).

The Flink connector supports DataStream API, Table API & SQL, and Python API. It has a higher and more stable performance than [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) provided by Apache Flink®.

> **NOTICE**
>
> Loading data into StarRocks tables with Flink connector needs SELECT and INSERT privileges on the target StarRocks table. If you do not have these privileges, follow the instructions provided in [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) to grant these privileges to the user that you use to connect to your StarRocks cluster.

## Version requirements

| Connector | Flink                         | StarRocks     | Java | Scala     |
|-----------|-------------------------------|---------------| ---- |-----------|
| 1.2.14    | 1.16,1.17,1.18,1.19,1.20      | 2.1 and later | 8    | 2.11,2.12 |
| 1.2.12    | 1.16,1.17,1.18,1.19,1.20      | 2.1 and later | 8    | 2.11,2.12 |
| 1.2.11    | 1.15,1.16,1.17,1.18,1.19,1.20 | 2.1 and later | 8    | 2.11,2.12 |
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19      | 2.1 and later | 8    | 2.11,2.12 |

## Obtain Flink connector

You can obtain the Flink connector JAR file in the following ways:

- Directly download the compiled Flink connector JAR file.
- Add the Flink connector as a dependency in your Maven project and then download the JAR file.
- Compile the source code of the Flink connector into a JAR file by yourself.

The naming format of the Flink connector JAR file is as follows:

- Since Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`. For example, if you install Flink 1.15 and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.15.jar`.

- Prior to Flink 1.15, it's `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`. For example, if you install Flink 1.14 and Scala 2.12 in your environment, and you want to use Flink connector 1.2.7, you can use `flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar`.

> **NOTICE**
>
> In general, the latest version of the Flink connector only maintains compatibility with the three most recent versions of Flink.

### Download the compiled Jar file

Directly download the corresponding version of the Flink connector Jar file from the [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks).

### Maven Dependency

In your Maven project's `pom.xml` file, add the Flink connector as a dependency according to the following format. Replace `flink_version`, `scala_version`, and `connector_version` with the respective versions.

- In Flink 1.15 and later

    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```

- In versions earlier than Flink 1.15

    ```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### Compile by yourself

1. Download the [Flink connector source code](https://github.com/StarRocks/starrocks-connector-for-apache-flink).
2. Execute the following command to compile the source code of Flink connector into a JAR file. Note that `flink_version` is replaced with the corresponding Flink version.

      ```bash
      sh build.sh <flink_version>
      ```

   For example, if the Flink version in your environment is 1.15, you need to execute the following command:

      ```bash
      sh build.sh 1.15
      ```

3. Go to the `target/` directory to find the Flink connector JAR file, such as `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`, generated upon compilation.

> **NOTE**
>
> The name of Flink connector which is not formally released contains the `SNAPSHOT` suffix.

## Options

### General Options

#### connector

- **Required**: Yes
- **Default value**: NONE
- **Description**: The connector that you want to use. The value must be "starrocks".

#### jdbc-url

- **Required**: Yes
- **Default value**: NONE
- **Description**: The address that is used to connect to the MySQL server of the FE. You can specify multiple addresses, which must be separated by a comma (,). Format: `jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`.

#### load-url

- **Required**: Yes
- **Default value**: NONE
- **Description**: The address that is used to connect to the HTTP server of the FE. You can specify multiple addresses, which must be separated by a semicolon (;). Format: `<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`.

#### database-name

- **Required**: Yes
- **Default value**: NONE
- **Description**: The name of the StarRocks database into which you want to load data.

#### table-name

- **Required**: Yes
- **Default value**: NONE
- **Description**: The name of the table that you want to use to load data into StarRocks.

#### username

- **Required**: Yes
- **Default value**: NONE
- **Description**: The username of the account that you want to use to load data into StarRocks. The account needs [SELECT and INSERT privileges](../sql-reference/sql-statements/account-management/GRANT.md) on the target StarRocks table.

#### password

- **Required**: Yes
- **Default value**: NONE
- **Description**: The password of the preceding account.

#### sink.version

- **Required**: No
- **Default value**: AUTO
- **Description**: The interface used to load data. This parameter is supported from Flink connector version 1.2.4 onwards. Valid Values:
  - `V1`: Use [Stream Load](../loading/StreamLoad.md) interface to load data. Connectors before 1.2.4 only support this mode. 
  - `V2`: Use [Stream Load transaction](./Stream_Load_transaction_interface.md) interface to load data. It requires StarRocks to be at least version 2.4. Recommends `V2` because it optimizes the memory usage and provides a more stable exactly-once implementation.
  - `AUTO`: If the version of StarRocks supports transaction Stream Load, will choose `V2` automatically, otherwise choose `V1`

#### sink.label-prefix

- **Required**: No
- **Default value**: NONE
- **Description**: The label prefix used by Stream Load. Recommend to configure it if you are using exactly-once with connector 1.2.8 and later. See [exactly-once usage notes](#exactly-once).

#### sink.semantic

- **Required**: No
- **Default value**: at-least-once
- **Description**: The semantic guaranteed by sink. Valid values: **at-least-once** and **exactly-once**.

#### sink.buffer-flush.max-bytes

- **Required**: No
- **Default value**: 94371840(90M)
- **Description**: The maximum size of data that can be accumulated in memory before being sent to StarRocks at a time. The maximum value ranges from 64 MB to 10 GB. Setting this parameter to a larger value can improve loading performance but may increase loading latency. This parameter only takes effect when `sink.semantic` is set to `at-least-once`. If `sink.semantic` is set to `exactly-once`, the data in memory is flushed when a Flink checkpoint is triggered. In this circumstance, this parameter does not take effect.

#### sink.buffer-flush.max-rows

- **Required**: No
- **Default value**: 500000
- **Description**: The maximum number of rows that can be accumulated in memory before being sent to StarRocks at a time. This parameter is available only when `sink.version` is `V1` and `sink.semantic` is `at-least-once`. Valid values: 64000 to 5000000.

#### sink.buffer-flush.interval-ms

- **Required**: No
- **Default value**: 300000
- **Description**: The interval at which data is flushed. This parameter is available only when `sink.semantic` is `at-least-once`. Unit: ms. Valid value range:
  - For versions earlier than v1.2.14: [1000, 3600000]
  - For v1.2.14 and later: (0, 3600000].

#### sink.max-retries

- **Required**: No
- **Default value**: 3
- **Description**: The number of times that the system retries to perform the Stream Load job. This parameter is available only when you set `sink.version` to `V1`. Valid values: 0 to 10.

#### sink.connect.timeout-ms

- **Required**: No
- **Default value**: 30000
- **Description**: The timeout for establishing HTTP connection. Valid values: 100 to 60000. Unit: ms. Before Flink connector v1.2.9, the default value is `1000`.

#### sink.socket.timeout-ms

- **Required**: No
- **Default value**: -1
- **Description**: Supported since 1.2.10. The time duration for which the HTTP client waits for data. Unit: ms. The default value `-1` means there is no timeout.

#### sink.sanitize-error-log

- **Required**: No
- **Default value**: false
- **Description**: Supported since 1.2.12. Whether to sanitize sensitive data in the error log for production security. When this item is set to `true`, sensitive row data and column values in Stream Load error logs are redacted in both the connector and SDK logs. The value defaults to `false` for backward compatibility.

#### sink.wait-for-continue.timeout-ms

- **Required**: No
- **Default value**: 10000
- **Description**: Supported since 1.2.7. The timeout for waiting response of HTTP 100-continue from the FE. Valid values: `3000` to `60000`. Unit: ms

#### sink.ignore.update-before

- **Required**: No
- **Default value**: true
- **Description**: Supported since version 1.2.8. Whether to ignore `UPDATE_BEFORE` records from Flink when loading data to Primary Key tables. If this parameter is set to false, the record is treated as a delete operation to StarRocks table.

#### sink.parallelism

- **Required**: No
- **Default value**: NONE
- **Description**: The parallelism of loading. Only available for Flink SQL. If this parameter is not specified, Flink planner decides the parallelism. **In the scenario of multi-parallelism, users need to guarantee data is written in the correct order.**

#### sink.properties.*

- **Required**: No
- **Default value**: NONE
- **Description**: The parameters that are used to control Stream Load behavior. For example, the parameter `sink.properties.format` specifies the format used for Stream Load, such as CSV or JSON. For a list of supported parameters and their descriptions, see [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md).

#### sink.properties.format

- **Required**: No
- **Default value**: csv
- **Description**: The format used for Stream Load. The Flink connector will transform each batch of data to the format before sending them to StarRocks. Valid values: `csv` and `json`.

#### sink.properties.column_separator  

- **Required**: No
- **Default value**: \t
- **Description**: The column separator for CSV-formatted data.

#### sink.properties.row_delimiter

- **Required**: No
- **Default value**: \n
- **Description**: The row delimiter for CSV-formatted data.

#### sink.properties.max_filter_ratio  

- **Required**: No
- **Default value**: 0
- **Description**: The maximum error tolerance of the Stream Load. It's the maximum percentage of data records that can be filtered out due to inadequate data quality. Valid values: `0` to `1`. Default value: `0`. See [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) for details.

#### sink.properties.partial_update

- **Required**: NO
- **Default value**: `FALSE`
- **Description**: Whether to use partial updates. Valid values: `TRUE` and `FALSE`. Default value: `FALSE`, indicating to disable this feature.

#### sink.properties.partial_update_mode

- **Required**:  NO
- **Default value**: `row`
- **Description**: Specifies the mode for partial updates. Valid values: `row` and `column`.
  - The value `row` (default) means partial updates in row mode, which is more suitable for real-time updates with many columns and small batches.
  - The value `column` means partial updates in column mode, which is more suitable for batch updates with few columns and many rows. In such scenarios, enabling the column mode offers faster update speeds. For example, in a table with 100 columns, if only 10 columns (10% of the total) are updated for all rows, the update speed of the column mode is 10 times faster.

#### sink.properties.strict_mode

- **Required**: No
- **Default value**: false
- **Description**: Specifies whether to enable the strict mode for Stream Load. It affects the loading behavior when there are unqualified rows, such as inconsistent column values. Valid values: `true` and `false`. Default value: `false`. See [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) for details.

#### sink.properties.compression

- **Required**: No
- **Default value**: NONE
- **Description**: The compression algorithm used for Stream Load. Valid values: `lz4_frame`. Compression for the JSON format requires Flink connector 1.2.10+ and StarRocks v3.2.7+. Compression for the CSV format only requires Flink connector 1.2.11+.

#### sink.properties.prepared_timeout

- **Required**: No
- **Default value**: NONE
- **Description**: Supported since 1.2.12 and only effective when `sink.version` is set to `V2`. Requires StarRocks 3.5.4 or later. Sets the timeout in seconds for the Transaction Stream Load phase from `PREPARED` to `COMMITTED`. Typically, only needed for exactly-once; at-least-once usually does not require setting this (the connector defaults to 300s). If not set in exactly-once, StarRocks FE configuration `prepared_transaction_default_timeout_second` (default 86400s) applies. See [StarRocks Transaction timeout management](./Stream_Load_transaction_interface.md#transaction-timeout-management).

#### sink.publish-timeout.ms

- **Required**: No
- **Default value**: -1
- **Description**: Supported since 1.2.14 and only effective when `sink.version` is set to `V2`. Timeout in milliseconds for the Publish phase. If a transaction stays in COMMITTED status longer than this timeout, the system will consider it as successful. The default value `-1` means using StarRocks server-side default behavior. When Merge Commit is enabled, the default timeout is 10000 ms.

### Merge Commit options

Supported from v1.2.14 onwards. Merge Commit allows the system to merge data from multiple subtasks into a single Stream Load transaction for better performance. You can enable this feature by setting `sink.properties.enable_merge_commit` to `true`. For more details about the merge commit feature in StarRocks, see [Merge Commit parameters](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#merge-commit-parameters).

The following Stream Load properties are used to control the Merge Commit behavior:

#### sink.properties.enable_merge_commit

- **Required**: No
- **Default value**: false
- **Description**: Whether to enable Merge Commit.

#### sink.properties.merge_commit_interval_ms

- **Required**: Yes (when Merge Commit is enabled)
- **Default value**: NONE
- **Description**: The Merge Commit time window in milliseconds. The system merges loading requests received within this window into a single transaction. A larger value improves merging efficiency but increases latency. This property must be set when `enable_merge_commit` is set to `true`.

#### sink.properties.merge_commit_parallel

- **Required**: No
- **Default value**: 3
- **Description**: The degree of parallelism for the loading plan created for each Merge Commit-enabled transaction. It is different from `sink.parallelism` which controls the parallelism of the Flink sink operator.

#### sink.properties.merge_commit_async

- **Required**: No
- **Default value**: true
- **Description**: The server's return mode for Merge Commit. The default value is `true` (asynchronous), overriding the system default behavior (synchronous) for better throughput. In the asynchronous mode, the server returns immediately after receiving the data. The connector leverages Flink's checkpoint mechanism to ensure no data loss under the asynchronous mode, providing at-least-once guarantee. In most cases, you do not need to change this value.

#### sink.merge-commit.max-concurrent-requests

- **Required**: No
- **Default value**: Integer.MAX_VALUE
- **Description**: The maximum number of concurrent Stream Load requests. Set this property to `0` to ensure in-order (serial) loading, which is useful for Primary Key tables. A negative value is treated as `Integer.MAX_VALUE` (unlimited concurrency).

#### sink.merge-commit.chunk.size

- **Required**: No
- **Default value**: 20971520
- **Description**: The maximum size of data (in bytes) accumulated in a chunk before it is flushed and sent to StarRocks via a Stream Load request. A larger value improves throughput but increases memory usage and latency; a smaller value reduces memory usage and latency but may lower throughput. When `max-concurrent-requests` is set to `0` (in-order mode), the default value of this property is changed to 500 MB because only one request runs at a time so a larger batch maximizes throughput.

## Data type mapping between Flink and StarRocks

| Flink data type                   | StarRocks data type   |
|-----------------------------------|-----------------------|
| BOOLEAN                           | BOOLEAN               |
| TINYINT                           | TINYINT               |
| SMALLINT                          | SMALLINT              |
| INTEGER                           | INTEGER               |
| BIGINT                            | BIGINT                |
| FLOAT                             | FLOAT                 |
| DOUBLE                            | DOUBLE                |
| DECIMAL                           | DECIMAL               |
| BINARY                            | INT                   |
| CHAR                              | STRING                |
| VARCHAR                           | STRING                |
| STRING                            | STRING                |
| DATE                              | DATE                  |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME              |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME              |
| ARRAY&lt;T&gt;                    | ARRAY&lt;T&gt;        |
| MAP&lt;KT,VT&gt;                  | JSON STRING           |
| ROW&lt;arg T...&gt;               | JSON STRING           |

## Usage notes

### Exactly Once

- If you want sink to guarantee exactly-once semantics, we recommend you to upgrade StarRocks to 2.5 or later, and Flink connector to 1.2.4 or later
  - Since Flink connector 1.2.4, the exactly-once is redesigned based on [Stream Load transaction interface](./Stream_Load_transaction_interface.md)
    provided by StarRocks since 2.4. Compared to the previous implementation based on non-transactional Stream Load non-transactional interface,
    the new implementation reduces memory usage and checkpoint overhead, thereby enhancing real-time performance and
    stability of loading.

  - If the version of StarRocks is earlier than 2.4 or the version of Flink connector is earlier than 1.2.4, the sink
    will automatically choose the implementation based on Stream Load non-transactional interface.

- Configurations to guarantee exactly-once

  - The value of `sink.semantic` needs to be `exactly-once`.

  - If the version of Flink connector is 1.2.8 and later, it is recommended to specify the value of `sink.label-prefix`. Note that the label prefix must be unique among all types of loading in StarRocks, such as Flink jobs, Routine Load, and Broker Load.

    - If the label prefix is specified, the Flink connector will use the label prefix to clean up lingering transactions that may be generated in some Flink
      failure scenarios, such as the Flink job fails when a checkpoint is still in progress. These lingering transactions
      are generally in `PREPARED` status if you use `SHOW PROC '/transactions/<db_id>/running';` to view them in StarRocks. When the Flink job restores from checkpoint,
      the Flink connector will find these lingering transactions according to the label prefix and some information in
      checkpoint, and abort them. The Flink connector can not abort them when the Flink job exits because of the two-phase-commit
      mechanism to implement the exactly-once. When the Flink job exits, the Flink connector has not received the notification from
      Flink checkpoint coordinator whether the transactions should be included in a successful checkpoint, and it may
      lead to data loss if these transactions are aborted anyway. You can have an overview about how to achieve end-to-end exactly-once
      in Flink in this [blogpost](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/).

    - If the label prefix is not specified, lingering transactions will be cleaned up by StarRocks only after they time out. However the number of running transactions can reach the limitation of StarRocks `max_running_txn_num_per_db` if
      Flink jobs fail frequently before transactions time out. You can set a smaller timeout for `PREPARED` transactions
      to make them expired faster when the label prefix is not specified. See the following about how to set the prepared timeout.

- If you are certain that the Flink job will eventually recover from checkpoint or savepoint after a long downtime because of stop or continuous failover,
  please adjust the following StarRocks configurations accordingly, to avoid data loss.

  - Adjust `PREPARED` transaction timeout. See the following about how to set the timeout.

    The timeout needs to be larger than the downtime of the Flink job. Otherwise, the lingering transactions that are included in a successful checkpoint may be aborted because of timeout before you restart the Flink job, which leads to data loss.

    Note that when you set a larger value to this configuration, it is better to specify the value of `sink.label-prefix` so that the lingering transactions can be cleaned according to the label prefix and some information in
      checkpoint, instead of due to timeout (which may cause data loss).

  - `label_keep_max_second` and `label_keep_max_num`: StarRocks FE configurations, default values are `259200` and `1000`
    respectively. For details, see [FE configurations](./loading_introduction/loading_considerations.md#fe-configurations). The value of `label_keep_max_second` needs to be larger than the downtime of the Flink job. Otherwise, the Flink connector can not check the state of transactions in StarRocks by using the transaction labels saved in the Flink's savepoint or checkpoint and figure out whether these transactions are committed or not, which may eventually lead to data loss.

- How to set the timeout for PREPARED transactions

  - For Connector 1.2.12+ and StarRocks 3.5.4+, you can set the timeout by configuring the connector parameter `sink.properties.prepared_timeout`. By default, the value is not set, and it falls back to the StarRocks FE's global configuration `prepared_transaction_default_timeout_second` (default value is `86400`).

  - For other versions of Connector or StarRocks, you can set the timeout by configuring the StarRocks FE's global configuration `prepared_transaction_default_timeout_second` (default value is `86400`).

### Flush Policy

The Flink connector will buffer the data in memory, and flush them in batch to StarRocks via Stream Load. How the flush
is triggered is different between at-least-once and exactly-once.

For at-least-once, the flush will be triggered when any of the following conditions are met:

- the bytes of buffered rows reaches the limit `sink.buffer-flush.max-bytes`
- the number of buffered rows reaches the limit `sink.buffer-flush.max-rows`. (Only valid for sink version V1)
- the elapsed time since the last flush reaches the limit `sink.buffer-flush.interval-ms`
- a checkpoint is triggered

For exactly-once, the flush only happens when a checkpoint is triggered.

### Merge Commit

Merge Commit helps scale throughput without proportionally increasing StarRocks transaction overhead. Without Merge Commit, each Flink sink subtask maintains its own Stream Load transaction, so increasing `sink.parallelism` leads to more concurrent transactions and higher I/O and Compaction costs on StarRocks. Conversely, keeping parallelism low limits the pipeline's overall capacity. With Merge Commit is enabled, data from multiple sink subtasks is merged into a single transaction within each Merge window. This allows you to increase `sink.parallelism` for higher throughput without increasing the number of transactions. For configuration examples, see [Load data with merge commit](#load-data-with-merge-commit).

Below are some important notes when using Merge Commit:

- **Single parallelism has no benefit**

  If the Flink sink parallelism is 1, enabling Merge Commit provides no benefit since there is only one subtask sending data. It may even introduce additional latency due to the Merge Commit time window on the server side.

- **Only at-least-once semantic**

  Merge Commit only guarantees at-least-once semantic. It does not support exactly-once semantic. Do not set `sink.semantic` to `exactly-once` when Merge Commit is enabled.

- **Ordering for Primary Key tables**

  By default, `sink.merge-commit.max-concurrent-requests` is `Integer.MAX_VALUE`, which means a single sink subtask may send multiple Stream Load requests concurrently. This can cause out-of-order loading, which may be problematic for Primary Key tables. To ensure in-order loading, set `sink.merge-commit.max-concurrent-requests` to `0`, which, however, will reduce throughput. Alternatively, you can use Conditional Update to prevent newer data from being overwritten by older data. For configuration examples, see [In-order loading for Primary Key tables](#in-order-loading-for-primary-key-tables).

- **End-to-end loading latency**

  The total loading latency consists of two parts:
  - **Connector batching latency**: Controlled by `sink.buffer-flush.interval-ms` and `sink.merge-commit.chunk.size`. Data is flushed from the connector when either the chunk size limit is reached or the flush interval elapses, whichever comes first. The maximum connector-side latency is `sink.buffer-flush.interval-ms`. A smaller `sink.buffer-flush.interval-ms` reduces connector-side latency but sends data in smaller batches.
  - **StarRocks merge window**: Controlled by `sink.properties.merge_commit_interval_ms`. The system waits for this duration to merge requests from multiple subtasks into a single transaction. A larger value improves merging efficiency (more requests will be merged into one transaction) but increases server-side latency.
  - As a general guideline, set `sink.buffer-flush.interval-ms` to be smaller than or equal to `sink.properties.merge_commit_interval_ms`, so that each subtask can flush at least once within each Merge window. For example, if `merge_commit_interval_ms` is `10000` (10s), you could set `sink.buffer-flush.interval-ms` to `5000` (5 seconds) or less.

- **Tuning `sink.parallelism` and `sink.properties.merge_commit_parallel`**

  These two parameters control parallelism at different layers and should be tuned independently:
  - `sink.parallelism` controls the number of Flink sink subtasks. Each subtask buffers and sends data to StarRocks. Increase this value when Flink sink operators are CPU- or memory-bound — you can monitor Flink's per-operator CPU and memory usage to determine whether more subtasks are needed.
  - `sink.properties.merge_commit_parallel` controls the degree of parallelism for the loading plan that StarRocks creates for each Merge Commit transaction. Increase this value when StarRocks becomes the bottleneck. You can monitor the StarRocks metrics [merge_commit_pending_total](../administration/management/monitoring/metrics.md#merge_commit_pending_total) (number of pending Merge Commit tasks) and [merge_commit_pending_bytes](../administration/management/monitoring/metrics.md#merge_commit_pending_bytes) (bytes held by pending tasks) to determine whether more parallelism is needed on the StarRocks side — sustained high values indicate that the loading plan cannot keep up with incoming data.

- **Relationship between `sink.merge-commit.chunk.size` and `sink.buffer-flush.max-bytes`**:
  - `sink.merge-commit.chunk.size` controls the maximum data size per individual Stream Load request (per chunk). When data in a chunk reaches this size, it is flushed immediately.
  - `sink.buffer-flush.max-bytes` controls the total memory limit for all cached data across all tables. When the total cached data exceeds this limit, the connector will evict chunks early to free memory.
  - Therefore, `sink.buffer-flush.max-bytes` should be set larger than `sink.merge-commit.chunk.size` to allow at least one full chunk to be accumulated. In general, `sink.buffer-flush.max-bytes` should be several times larger than `sink.merge-commit.chunk.size`, especially when there are multiple tables or high concurrency.

### Monitoring load metrics

The Flink connector provides the following metrics to monitor loading.

| Metric                   | Type    | Description                                                     |
|--------------------------|---------|-----------------------------------------------------------------|
| totalFlushBytes          | counter | successfully flushed bytes.                                     |
| totalFlushRows           | counter | number of rows successfully flushed.                            |
| totalFlushSucceededTimes | counter | number of times that the data is successfully flushed.          |
| totalFlushFailedTimes    | counter | number of times that the data fails to be flushed.              |
| totalFilteredRows        | counter | number of rows filtered, which is also included in totalFlushRows. |

## Examples

The following examples show how to use the Flink connector to load data into a StarRocks table with Flink SQL or Flink DataStream.

### Preparations

#### Create a StarRocks table

Create a database `test` and create a Primary Key table `score_board`.

```sql
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### Set up Flink environment

- Download Flink binary [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz), and unzip it to directory `flink-1.15.2`.
- Download [Flink connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar), and put it into the directory `flink-1.15.2/lib`.
- Run the following commands to start a Flink cluster:

    ```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

#### Network configuration

Ensure that the machine where Flink is located can access the FE nodes of the StarRocks cluster via the [`http_port`](../administration/management/FE_configuration.md#http_port) (default: `8030`) and [`query_port`](../administration/management/FE_configuration.md#query_port) (default: `9030`), and the BE nodes via the [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) (default: `8040`).

### Run with Flink SQL

- Run the following command to start a Flink SQL client.

    ```shell
    ./bin/sql-client.sh
    ```

- Create a Flink table `score_board`, and insert values into the table via Flink SQL Client.
Note you must define the primary key in the Flink DDL if you want to load data into a Primary Key table of StarRocks. It's optional for other types of StarRocks tables.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = ''
    );

    INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    ```

### Run with Flink DataStream

There are several ways to implement a Flink DataStream job according to the type of the input records, such as a CSV Java `String`, a JSON Java `String` or a custom Java object.

- The input records are CSV-format `String`. See [LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) for a complete example.

    ```java
    /**
     * Generate CSV-format records. Each record has three values separated by "\t". 
     * These values will be loaded to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "1\tstarrocks-csv\t100",
            "2\tflink-csv\t100"
    };
    DataStream<String> source = env.fromElements(records);

    /**
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.column_separator"
     * to tell the connector the input records are CSV-format, and the column separator is "\t".
     * You can also use other column separators in the CSV-format records,
     * but remember to modify the "sink.properties.column_separator" correspondingly.
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "csv")
            .withProperty("sink.properties.column_separator", "\t")
            .build();
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- The input records are JSON-format `String`. See [LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) for a complete example.

    ```java
    /**
     * Generate JSON-format records. 
     * Each record has three key-value pairs corresponding to the columns `id`, `name`, and `score` in the StarRocks table.
     */
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);

    /** 
     * Configure the connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.strip_outer_array"
     * to tell the connector the input records are JSON-format and to strip the outermost array structure. 
     */
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .withProperty("sink.properties.format", "json")
            .withProperty("sink.properties.strip_outer_array", "true")
            .build();
    // Create the sink with the options.
    SinkFunction<String> starRockSink = StarRocksSink.sink(options);
    source.addSink(starRockSink);
    ```

- The input records are custom Java objects. See [LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) for a complete example.

  - In this example, the input record is a simple POJO `RowData`.

      ```java
      public static class RowData {
              public int id;
              public String name;
              public int score;
    
              public RowData() {}
    
              public RowData(int id, String name, int score) {
                  this.id = id;
                  this.name = name;
                  this.score = score;
              }
        }
      ```

  - The main program is as follows:

    ```java
    // Generate records which use RowData as the container.
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);

    // Configure the connector with the required properties.
    StarRocksSinkOptions options = StarRocksSinkOptions.builder()
            .withProperty("jdbc-url", jdbcUrl)
            .withProperty("load-url", loadUrl)
            .withProperty("database-name", "test")
            .withProperty("table-name", "score_board")
            .withProperty("username", "root")
            .withProperty("password", "")
            .build();

    /**
     * The Flink connector will use a Java object array (Object[]) to represent a row to be loaded into the StarRocks table,
     * and each element is the value for a column.
     * You need to define the schema of the Object[] which matches that of the StarRocks table.
     */
    TableSchema schema = TableSchema.builder()
            .field("id", DataTypes.INT().notNull())
            .field("name", DataTypes.STRING())
            .field("score", DataTypes.INT())
            // When the StarRocks table is a Primary Key table, you must specify notNull(), for example, DataTypes.INT().notNull(), for the primary key `id`.
            .primaryKey("id")
            .build();
    // Transform the RowData to the Object[] according to the schema.
    RowDataTransformer transformer = new RowDataTransformer();
    // Create the sink with the schema, options, and transformer.
    SinkFunction<RowData> starRockSink = StarRocksSink.sink(schema, options, transformer);
    source.addSink(starRockSink);
    ```

  - The `RowDataTransformer` in the main program is defined as follows:

    ```java
    private static class RowDataTransformer implements StarRocksSinkRowBuilder<RowData> {
    
        /**
         * Set each element of the object array according to the input RowData.
         * The schema of the array matches that of the StarRocks table.
         */
        @Override
        public void accept(Object[] internalRow, RowData rowData) {
            internalRow[0] = rowData.id;
            internalRow[1] = rowData.name;
            internalRow[2] = rowData.score;
            // When the StarRocks table is a Primary Key table, you need to set the last element to indicate whether the data loading is an UPSERT or DELETE operation.
            internalRow[internalRow.length - 1] = StarRocksSinkOP.UPSERT.ordinal();
        }
    }  
    ```

### Synchronize data with Flink CDC 3.0 (with schema change supported)

[Flink CDC 3.0](https://nightlies.apache.org/flink/flink-cdc-docs-stable) framework can be used
to easily build a streaming ELT pipeline from CDC sources (such as MySQL and Kafka) to StarRocks. The pipeline can synchronize whole database, merged sharding tables, and schema changes from sources to StarRocks.

Since v1.2.9, the Flink connector for StarRocks is integrated into this framework as [StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/pipeline-connectors/starrocks/). The StarRocks Pipeline Connector supports:

- Automatic creation of databases and tables
- Schema change synchronization
- Full and incremental data synchronization

For quick start, see [Streaming ELT from MySQL to StarRocks using Flink CDC 3.0 with StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/quickstart/mysql-to-starrocks).

It is advised to use StarRocks v3.2.1 and later versions to enable [fast_schema_evolution](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution). It will improve the speed of adding or dropping columns and reduce resource usage.

## Best practices

### Load data to a Primary Key table

This section will show how to load data to a StarRocks Primary Key table to achieve partial updates and conditional updates.
You can see [Change data through loading](./Load_to_Primary_Key_tables.md) for the introduction of those features.
These examples use Flink SQL.

#### Preparations

Create a database `test` and create a Primary Key table `score_board` in StarRocks.

```SQL
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### Partial update

This example will show how to load data only to columns `id` and `name`.

1. Insert two data rows into the StarRocks table `score_board` in MySQL client.

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);

    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Create a Flink table `score_board` in Flink SQL client.

   - Define the DDL which only includes the columns `id` and `name`.
   - Set the option `sink.properties.partial_update` to `true` which tells the Flink connector to perform partial updates.
   - If the Flink connector version `<=` 1.2.7, you also need to set the option `sink.properties.columns` to `id,name,__op` to tells the Flink connector which columns need to be updated. Note that you need to append the field `__op` at the end. The field `__op` indicates that the data loading is an UPSERT or DELETE operation, and its values are set by the connector automatically.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.partial_update' = 'true',
        -- only for Flink connector version <= 1.2.7
        'sink.properties.columns' = 'id,name,__op'
    ); 
    ```

3. Insert two data rows into the Flink table. The primary keys of the data rows are as same as these of rows in the StarRocks table. but the values in the column `name` are modified.

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'flink-update');
    ```

4. Query the StarRocks table in MySQL client.
  
    ```SQL
    mysql> select * from score_board;
    +------+------------------+-------+
    | id   | name             | score |
    +------+------------------+-------+
    |    1 | starrocks-update |   100 |
    |    2 | flink-update     |   100 |
    +------+------------------+-------+
    2 rows in set (0.02 sec)
    ```

    You can see that only values for `name` change, and the values for `score` do not change.

#### Conditional update

This example will show how to do conditional update according to the value of column `score`. The update for an `id`
takes effect only when the new value for `score` is has a greater or equal to the old value.

1. Insert two data rows into the StarRocks table in MySQL client.

    ```SQL
    mysql> INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 100);
    
    mysql> select * from score_board;
    +------+-----------+-------+
    | id   | name      | score |
    +------+-----------+-------+
    |    1 | starrocks |   100 |
    |    2 | flink     |   100 |
    +------+-----------+-------+
    2 rows in set (0.02 sec)
    ```

2. Create a Flink table `score_board` in the following ways:
  
    - Define the DDL including all of columns.
    - Set the option `sink.properties.merge_condition` to `score` to tell the connector to use the column `score`
    as the condition.
    - Set the option `sink.version` to `V1` which tells the connector to use Stream Load.

    ```SQL
    CREATE TABLE `score_board` (
        `id` INT,
        `name` STRING,
        `score` INT,
        PRIMARY KEY (id) NOT ENFORCED
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'score_board',
        'username' = 'root',
        'password' = '',
        'sink.properties.merge_condition' = 'score',
        'sink.version' = 'V1'
        );
    ```

3. Insert two data rows into the Flink table. The primary keys of the data rows are as same as these of rows in the StarRocks table. The first data row has a smaller value in the column `score`, and the second data row has a larger  value in the column `score`.

    ```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'flink-update', 101);
    ```

4. Query the StarRocks table in MySQL client.

    ```SQL
    mysql> select * from score_board;
    +------+--------------+-------+
    | id   | name         | score |
    +------+--------------+-------+
    |    1 | starrocks    |   100 |
    |    2 | flink-update |   101 |
    +------+--------------+-------+
    2 rows in set (0.03 sec)
    ```

   You can see that only the values of the second data row change, and the values of the first data row do not change.

### Load data with Merge Commit

This section shows how to use Merge Commit to improve loading throughput when you have multiple Flink sink subtasks writing to the same StarRocks table. These examples use Flink SQL and StarRocks v3.4.0 or later.

#### Preparations

Create a database `test` and create a Primary Key table `score_board` in StarRocks.

```SQL
CREATE DATABASE `test`;

CREATE TABLE `test`.`score_board`
(
    `id` int(11) NOT NULL COMMENT "",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(`id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`);
```

#### Basic configuration

This Flink SQL enables merge commit with a 10-second merge window. Data from all sink subtasks is merged into a single transaction within each window.

```SQL
CREATE TABLE `score_board` (
    `id` INT,
    `name` STRING,
    `score` INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
    'load-url' = '127.0.0.1:8030',
    'database-name' = 'test',
    'table-name' = 'score_board',
    'username' = 'root',
    'password' = '',
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000'
);
```

Insert data into the Flink table. The data will be loaded into StarRocks via merge commit.

```SQL
INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

#### In-order loading for Primary Key tables

By default, a single sink subtask may send multiple Stream Load requests concurrently, which can cause out-of-order loading. For Primary Key tables where data ordering matters, there are two approaches to handle this issue.

**Method 1: Use `sink.merge-commit.max-concurrent-requests`**

Set `sink.merge-commit.max-concurrent-requests` to `0` to ensure each subtask sends requests one at a time. This guarantees in-order loading but may reduce throughput.

```SQL
CREATE TABLE `score_board` (
    `id` INT,
    `name` STRING,
    `score` INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
    'load-url' = '127.0.0.1:8030',
    'database-name' = 'test',
    'table-name' = 'score_board',
    'username' = 'root',
    'password' = '',
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.merge-commit.max-concurrent-requests' = '0'
);

INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

**Method 2: Use Conditional Update**

If you want to keep concurrent requests for higher throughput but still prevent older data from overwriting newer data, you can use [Conditional Update](#conditional-update). Set `sink.properties.merge_condition` to a column (for example, a version or timestamp column) so that an update only takes effect when the incoming value is greater than or equal to the existing value.

```SQL
CREATE TABLE `score_board` (
    `id` INT,
    `name` STRING,
    `score` INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'starrocks',
    'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
    'load-url' = '127.0.0.1:8030',
    'database-name' = 'test',
    'table-name' = 'score_board',
    'username' = 'root',
    'password' = '',
    'sink.properties.enable_merge_commit' = 'true',
    'sink.properties.merge_commit_interval_ms' = '10000',
    'sink.buffer-flush.interval-ms' = '5000',
    'sink.properties.merge_condition' = 'score'
);

INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

With this configuration, concurrent requests are allowed (default `sink.merge-commit.max-concurrent-requests` is `Integer.MAX_VALUE`), but an update to a row only takes effect when the new `score` is greater than or equal to the existing `score`. This prevents newer data from being overwritten by older data even under out-of-order loading.

### Load data into columns of BITMAP type

[`BITMAP`](../sql-reference/data-types/other-data-types/BITMAP.md) is often used to accelerate count distinct, such as counting UV, see [Use Bitmap for exact Count Distinct](../using_starrocks/distinct_values/Using_bitmap.md).
Here we take the counting of UV as an example to show how to load data into columns of the `BITMAP` type.

1. Create a StarRocks Aggregate table in MySQL client.

   In the database `test`, create an Aggregate table `page_uv` where the column `visit_users` is defined as the `BITMAP` type and configured with the aggregate function `BITMAP_UNION`.

    ```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Create a Flink table in Flink SQL client.

    The column `visit_user_id` in the Flink table is of `BIGINT` type, and we want to load this column to the column `visit_users` of `BITMAP` type in the StarRocks table. So when defining the DDL of the Flink table, note that:
    - Because Flink does not support `BITMAP`, you need to define a column `visit_user_id` as `BIGINT` type to represent the column `visit_users` of `BITMAP` type in the StarRocks table.
    - You need to set the option `sink.properties.columns` to `page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)`, which tells the connector the column mapping between the Flink table and StarRocks table. Also you need to use [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md)
   function to tell the connector to convert the data of `BIGINT` type into `BITMAP` type.

    ```SQL
    CREATE TABLE `page_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'page_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=to_bitmap(visit_user_id)'
    );
    ```

3. Load data into Flink table in Flink SQL client.

    ```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. Calculate page UVs from the StarRocks table in MySQL client.

    ```SQL
    MySQL [test]> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `page_uv` GROUP BY `page_id`;
    +---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       2 |                           1 |
    |       1 |                           3 |
    +---------+-----------------------------+
    2 rows in set (0.05 sec)
    ```

### Load data into columns of HLL type

[`HLL`](../sql-reference/data-types/other-data-types/HLL.md) can be used for approximate count distinct, see [Use HLL for approximate count distinct](../using_starrocks/distinct_values/Using_HLL.md).

Here we take the counting of UV as an example to show how to load data into columns of the `HLL` type.

1. Create a StarRocks Aggregate table

   In the database `test`, create an Aggregate table `hll_uv` where the column `visit_users` is defined as the `HLL` type and configured with the aggregate function `HLL_UNION`.

    ```SQL
    CREATE TABLE `hll_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. Create a Flink table in Flink SQL client.

   The column `visit_user_id` in the Flink table is of `BIGINT` type, and we want to load this column to the column `visit_users` of `HLL` type in the StarRocks table. So when defining the DDL of the Flink table, note that:
    - Because Flink does not support `BITMAP`, you need to define a column `visit_user_id` as `BIGINT` type to represent the column `visit_users` of `HLL` type in the StarRocks table.
    - You need to set the option `sink.properties.columns` to `page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)` which tells the connector the column mapping between Flink table and StarRocks table.  Also you need to use [`hll_hash`](../sql-reference/sql-functions/scalar-functions/hll_hash.md) function to tell the connector to convert the data of `BIGINT` type into `HLL` type.

    ```SQL
    CREATE TABLE `hll_uv` (
        `page_id` INT,
        `visit_date` TIMESTAMP,
        `visit_user_id` BIGINT
    ) WITH (
        'connector' = 'starrocks',
        'jdbc-url' = 'jdbc:mysql://127.0.0.1:9030',
        'load-url' = '127.0.0.1:8030',
        'database-name' = 'test',
        'table-name' = 'hll_uv',
        'username' = 'root',
        'password' = '',
        'sink.properties.columns' = 'page_id,visit_date,visit_user_id,visit_users=hll_hash(visit_user_id)'
    );
    ```

3. Load data into Flink table in Flink SQL client.

    ```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. Calculate page UVs from the StarRocks table in MySQL client.

    ```SQL
    mysql> SELECT `page_id`, COUNT(DISTINCT `visit_users`) FROM `hll_uv` GROUP BY `page_id`;
    **+---------+-----------------------------+
    | page_id | count(DISTINCT visit_users) |
    +---------+-----------------------------+
    |       3 |                           2 |
    |       4 |                           1 |
    +---------+-----------------------------+
    2 rows in set (0.04 sec)
    ```
