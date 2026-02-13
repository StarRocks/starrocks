---
displayed_sidebar: docs
---

# 从 Apache Flink® 持续导入数据

StarRocks 提供了一个自研的连接器，名为 StarRocks Connector for Apache Flink®（简称 Flink connector），以帮助您使用 Flink 将数据导入到 StarRocks 表中。其基本原理是先积累数据，然后通过 [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) 一次性将数据加载到 StarRocks 中。

Flink connector 支持 DataStream API、Table API & SQL 和 Python API。与 Apache Flink® 提供的 [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/) 相比，它具有更高、更稳定的性能。

> **注意**
>
> 使用 Flink connector 将数据导入到 StarRocks 表中，需要对目标 StarRocks 表具有 SELECT 和 INSERT 权限。如果您没有这些权限，请按照 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 中提供的说明，将这些权限授予用于连接 StarRocks 集群的用户。

## 版本要求

| Connector | Flink                         | StarRocks     | Java | Scala     |
|-----------|-------------------------------|---------------| ---- |-----------|
| 1.2.14    | 1.16,1.17,1.18,1.19,1.20      | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.12    | 1.16,1.17,1.18,1.19,1.20      | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.11    | 1.15,1.16,1.17,1.18,1.19,1.20 | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19      | 2.1 及更高版本 | 8    | 2.11,2.12 |

## 获取 Flink connector

您可以通过以下方式获取 Flink connector 的 JAR 文件：

- 直接下载已编译的 Flink connector JAR 文件。
- 在您的 Maven 项目中添加 Flink connector 作为依赖项，然后下载 JAR 文件。
- 自行将 Flink connector 的源代码编译为 JAR 文件。

Flink connector JAR 文件的命名格式如下：

- 从 Flink 1.15 开始，命名格式为 `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`。例如，如果您安装了 Flink 1.15 并且想要使用 Flink connector 1.2.7，则可以使用 `flink-connector-starrocks-1.2.7_flink-1.15.jar`。

- 在 Flink 1.15 之前，命名格式为 `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`。例如，如果您在您的环境中安装了 Flink 1.14 和 Scala 2.12，并且想要使用 Flink connector 1.2.7，则可以使用 `flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar`。

> **NOTICE**
>
> 通常，最新版本的 Flink connector 仅保持与最近三个版本的 Flink 的兼容性。

### 下载已编译的 Jar 文件

您可以直接从 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 下载相应版本的 Flink connector Jar 文件。

### Maven 依赖

在您的 Maven 项目的 `pom.xml` 文件中，按照以下格式添加 Flink connector 作为依赖。将 `flink_version`、`scala_version` 和 `connector_version` 替换为相应的版本。

- 在 Flink 1.15 及更高版本中

```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```

- 在 Flink 1.15 之前的版本中

```xml
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### 自行编译

1. 下载 [Flink connector 源代码](https://github.com/StarRocks/starrocks-connector-for-apache-flink) 。
2. 执行以下命令将 Flink connector 的源代码编译为 JAR 文件。请注意，`flink_version` 替换为相应的 Flink 版本。

```bash
      sh build.sh <flink_version>
      ```

例如，如果您的环境中 Flink 的版本为 1.15，则需要执行以下命令：

```bash
      sh build.sh 1.15
      ```

3.  进入 `target/` 目录，找到编译后生成的 Flink connector JAR 文件，例如 `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`。

> **说明**
>
> 如果 Flink connector 未正式发布，其名称会带有 `SNAPSHOT` 后缀。

## Options

### 常用选项

#### connector

- **是否必填**: 是
- **默认值**: NONE
- **描述**: 您要使用的连接器。该值必须为 "starrocks"。

#### jdbc-url

- **是否必填**: 是
- **默认值**: NONE
- **描述**: 用于连接 FE 的 MySQL 服务器的地址。您可以指定多个地址，地址之间必须使用英文逗号 (,) 分隔。格式：`jdbc:mysql://<fe_host1>:<fe_query_port1>,<fe_host2>:<fe_query_port2>,<fe_host3>:<fe_query_port3>`。

#### load-url

- **是否必填**：是
- **默认值**：无
- **描述**：用于连接 FE 的 HTTP 服务的地址。您可以指定多个地址，地址之间使用分号 (;) 分隔。格式：`<fe_host1>:<fe_http_port1>;<fe_host2>:<fe_http_port2>`。

#### database-name

- **是否必填**：是
- **默认值**：无
- **描述**：您要将数据导入的 StarRocks 数据库的名称。

#### table-name

- **是否必填**：是
- **默认值**：无
- **描述**：您要将数据导入到 StarRocks 中的表的名称。

#### username

- **是否必填**：是
- **默认值**：无
- **描述**：用于将数据导入到 StarRocks 中的帐户的用户名。该帐户需要具有目标 StarRocks 表的 [SELECT 和 INSERT 权限](../sql-reference/sql-statements/account-management/GRANT.md) 。

#### password

- **是否必填**：是
- **默认值**：无
- **描述**：上述账号的密码。

#### sink.version

- **是否必填**：否
- **默认值**：AUTO
- **描述**：用于数据导入的接口。该参数自 Flink connector 1.2.4 版本起支持。取值范围：
  - `V1`: 使用 [Stream Load](../loading/StreamLoad.md) 接口导入数据。1.2.4 之前的 Connector 仅支持此模式。
  - `V2`: 使用 [Stream Load transaction](./Stream_Load_transaction_interface.md) 接口导入数据。要求 StarRocks 版本至少为 2.4。推荐使用 `V2`，因为它优化了内存使用，并提供了更稳定的 exactly-once 实现。
  - `AUTO`: 如果 StarRocks 版本支持事务 Stream Load，则自动选择 `V2`，否则选择 `V1`。

#### sink.label-prefix

- **是否必填**：否
- **默认值**：无
- **描述**：Stream Load使用的标签前缀。如果您正在使用connector 1.2.8及更高版本的exactly-once，建议您配置它。请参见 [exactly-once 使用说明](#exactly-once)。

#### sink.semantic

- **是否必填**: 否
- **默认值**: at-least-once
- **描述**: sink 提供的语义保障。有效值：**at-least-once** 和 **exactly-once**。

#### sink.buffer-flush.max-bytes

- **是否必须配置**：否
- **默认值**：94371840(90M)
- **描述**：在一次性发送到 StarRocks 之前，可以在内存中累积的最大数据量。最大值的范围是 64 MB 到 10 GB。将此参数设置为较大的值可以提高数据导入性能，但也可能会增加数据导入延迟。此参数仅在 `sink.semantic` 设置为 `at-least-once` 时生效。如果 `sink.semantic` 设置为 `exactly-once`，则会在触发 Flink checkpoint 时刷新内存中的数据。在这种情况下，此参数不生效。

#### sink.buffer-flush.max-rows

- **是否必填**：否
- **默认值**：500000
- **描述**：一次发送到 StarRocks 之前可以在内存中累积的最大行数。此参数仅在 `sink.version` 为 `V1` 且 `sink.semantic` 为 `at-least-once` 时可用。有效值：64000 到 5000000。

#### sink.buffer-flush.interval-ms

- **是否必填**: 否
- **默认值**: 300000
- **描述**: 数据刷新的间隔。仅当 `sink.semantic` 为 `at-least-once` 时，此参数才可用。单位：毫秒。有效取值范围：
  - v1.2.14 之前的版本：[1000, 3600000]
  - v1.2.14 及更高版本：(0, 3600000]

#### sink.max-retries

- **是否必填**：否
- **默认值**：3
- **描述**：系统重试执行 Stream Load 作业的次数。仅当您将 `sink.version` 设置为 `V1` 时，此参数才可用。有效值：0 到 10。

#### sink.connect.timeout-ms

- **是否必填**：否
- **默认值**：30000
- **描述**：建立 HTTP 连接的超时时间。有效值：100 到 60000。单位：毫秒。在 Flink connector v1.2.9 之前的版本中，默认值为 `1000`。

#### sink.socket.timeout-ms

- **是否必填**：否
- **默认值**：-1
- **描述**：自 1.2.10 版本起支持。HTTP 客户端等待数据的时间。单位：毫秒。默认值 `-1` 表示没有超时时间。

#### sink.sanitize-error-log

- **Required**: No
- **Default value**: false
- **Description**:  自 1.2.12 版本起支持。是否对生产环境安全相关的错误日志中的敏感数据进行脱敏。如果设置为 `true`，连接器和 SDK 日志中的 Stream Load 错误日志中的敏感行数据和列值将被删除。为了向后兼容，该值默认为 `false`。

#### sink.wait-for-continue.timeout-ms

- **是否必填**：否
- **默认值**：10000
- **描述**：自 1.2.7 版本起支持。等待 FE 返回 HTTP 100-continue 响应的超时时间。取值范围：`3000` 到 `60000`。单位：毫秒（ms）。

#### sink.ignore.update-before

- **是否必填**：否
- **默认值**：true
- **描述**：自 1.2.8 版本起支持。是否在向主键表导入数据时忽略来自 Flink 的 `UPDATE_BEFORE` 类型记录。如果设置为 false，则该记录会被当做删除操作。

#### sink.parallelism

- **是否必填**：否
- **默认值**：NONE
- **描述**：数据导入的并行度。仅适用于 Flink SQL。如果未指定此参数，则由 Flink planner 决定并行度。**在多并行度的情况下，用户需要保证数据以正确的顺序写入。**

#### sink.properties.*

- **是否必填**：否
- **默认值**：无
- **描述**：用于控制 Stream Load 行为的参数。例如，参数 `sink.properties.format` 指定用于 Stream Load 的格式，例如 CSV 或 JSON。有关支持的参数及其描述的列表，请参见 [STREAM LOAD](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) 。

#### sink.properties.format

- **是否必填**：否
- **默认值**：csv
- **描述**：用于 Stream Load 的数据格式。Flink Connector 会将每批数据转换为指定格式，然后再发送到 StarRocks。有效值：`csv` 和 `json`。

#### sink.properties.column_separator

- **是否必填**: 否
- **默认值**: \t
- **描述**: CSV 格式数据的列分隔符。

#### sink.properties.row_delimiter

- **是否必填**：否
- **默认值**：\n
- **描述**：CSV 格式数据中的行分隔符。

#### sink.properties.max_filter_ratio

- **是否必填**：否
- **默认值**：0
- **描述**：Stream Load 的最大容错率。表示因数据质量不合格而允许过滤掉的数据记录的最大百分比。取值范围：`0` ~ `1`。默认值：`0`。更多信息，请参见 [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) 。

#### sink.properties.partial_update

- **是否必填**：否
- **默认值**：`FALSE`
- **描述**：是否使用部分更新。有效值为 `TRUE` 和 `FALSE`。默认值为 `FALSE`，表示禁用此功能。

#### sink.properties.partial_update_mode

- **是否必填**：否
- **默认值**：`row`
- **描述**：指定部分更新的模式。有效值：`row` 和 `column`。
  -  `row`（默认值）表示行模式下的部分更新，更适合多列、小批量的实时更新。
  -  `column` 表示列模式下的部分更新，更适合少列、多行的批量更新。在这种情况下，启用列模式可以提供更快的更新速度。例如，在一张有 100 列的表中，如果只更新所有行的 10 列（总列数的 10%），那么列模式的更新速度会快 10 倍。

#### sink.properties.strict_mode

- **是否必填**：否
- **默认值**：false
- **描述**：是否开启 Stream Load 的严格模式。它会影响存在不合格行（例如列值不一致）时的数据导入行为。有效值：`true` 和 `false`。默认值：`false`。详情请参见 [Stream Load](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md) 。

#### sink.properties.compression

- **是否必填**: 否
- **默认值**: NONE
- **描述**: 用于 Stream Load 的压缩算法。有效值：`lz4_frame`。JSON 格式的压缩需要 Flink connector 1.2.10+ 和 StarRocks v3.2.7+。CSV 格式的压缩只需要 Flink connector 1.2.11+。

#### sink.properties.prepared_timeout

- **是否必填**：否
- **默认值**：NONE
- **描述**：自 1.2.12 版本起支持，仅当 `sink.version` 设置为 `V2` 时生效。需要 StarRocks 3.5.4 或更高版本。设置从 `PREPARED` 到 `COMMITTED` 的事务性 Stream Load 阶段的超时时间，单位为秒。通常，仅在 exactly-once 语义时需要设置；at-least-once 语义通常不需要设置此项（连接器默认为 300 秒）。如果在 exactly-once 语义中未设置，则应用 StarRocks FE 配置 `prepared_transaction_default_timeout_second`（默认为 86400 秒）。请参阅 [StarRocks 事务超时管理](./Stream_Load_transaction_interface.md#transaction-timeout-management) 。

#### sink.publish-timeout.ms

- **是否必填**：否
- **默认值**：-1
- **描述**：自 1.2.14 版本起支持，且仅当 `sink.version` 设置为 `V2` 时生效。Publish 阶段的超时时间，单位为毫秒。如果事务保持在 COMMITTED 状态的时间超过此超时时间，系统将认为该事务已成功。默认值 `-1` 表示使用 StarRocks 服务器端的默认行为。当启用 Merge Commit 时，默认超时时间为 10000 毫秒。

### Merge Commit 选项

从 v1.2.14 版本开始支持。Merge Commit 允许系统将来自多个子任务的数据合并到单个 Stream Load 事务中，以获得更好的性能。您可以通过将 `sink.properties.enable_merge_commit` 设置为 `true` 来启用此功能。有关 StarRocks 中 merge commit 功能的更多详细信息，请参见 [Merge Commit 参数](../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#merge-commit-parameters) 。

以下 Stream Load 属性用于控制 Merge Commit 的行为：

#### sink.properties.enable_merge_commit

- **是否必填**：否
- **默认值**：false
- **描述**：是否开启 Merge Commit。

#### sink.properties.merge_commit_interval_ms

- **是否必填**：是（当启用 Merge Commit 时）
- **默认值**：无
- **描述**：Merge Commit 的时间窗口，单位为毫秒。系统会将在此窗口内收到的数据导入请求合并到单个事务中。较大的值可以提高合并效率，但会增加延迟。当 `enable_merge_commit` 设置为 `true` 时，必须设置此属性。

#### sink.properties.merge_commit_parallel

- **是否必填**：否
- **默认值**：3
- **描述**：为每个启用 Merge Commit 的事务创建的导入计划的并行度。它与控制 Flink sink operator 的并行度的 `sink.parallelism` 不同。

#### sink.properties.merge_commit_async

- **是否必须**: 否
- **默认值**: true
- **描述**: 服务器对 Merge Commit 的返回模式。 默认值为 `true` (异步)，覆盖系统默认行为（同步），以获得更好的吞吐量。 在异步模式下，服务器在收到数据后立即返回。 连接器利用 Flink 的 checkpoint 机制来确保异步模式下不会丢失数据，从而提供至少一次的保证。 在大多数情况下，您不需要更改此值。

#### sink.merge-commit.max-concurrent-requests

- **是否必须**：否
- **默认值**：Integer.MAX_VALUE
- **描述**：并发 Stream Load 请求的最大数量。将此属性设置为 `0` 可确保按顺序（串行）导入，这对于主键表非常有用。负值被视为 `Integer.MAX_VALUE`（无限制并发）。

#### sink.merge-commit.chunk.size

- **是否必填**：否
- **默认值**：20971520
- **描述**：在刷新并通过 Stream Load 请求发送到 StarRocks 之前，一个 chunk 中累积的最大数据量（以字节为单位）。较大的值可以提高吞吐量，但会增加内存使用量和延迟；较小的值会减少内存使用量和延迟，但可能会降低吞吐量。当 `max-concurrent-requests` 设置为 `0`（顺序模式）时，此属性的默认值将更改为 500 MB，因为一次只运行一个请求，因此更大的批处理可以最大限度地提高吞吐量。

## Flink 和 StarRocks 之间的数据类型映射

| Flink 数据类型                    | StarRocks 数据类型    |
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

## 使用须知

### Exactly Once

- 如果您希望 sink 保证 exactly-once 语义，我们建议您将 StarRocks 升级到 2.5 或更高版本，并将 Flink connector 升级到 1.2.4 或更高版本。
  - 从 Flink connector 1.2.4 开始，exactly-once 是基于 StarRocks 从 2.4 开始提供的 [Stream Load transaction interface](./Stream_Load_transaction_interface.md) 重新设计的。与之前基于非事务性 Stream Load 接口的实现相比，新实现减少了内存使用和 checkpoint 开销，从而提高了数据导入的实时性和稳定性。

  - 如果 StarRocks 的版本早于 2.4 或 Flink connector 的版本早于 1.2.4，则 sink 将自动选择基于非事务性 Stream Load 接口的实现。

- 保证 exactly-once 的配置

  - `sink.semantic` 的值需要为 `exactly-once`。

  - 如果 Flink connector 的版本为 1.2.8 及更高版本，建议指定 `sink.label-prefix` 的值。请注意，label prefix 在 StarRocks 的所有数据导入类型（例如 Flink 作业、Routine Load 和 Broker Load）中必须是唯一的。

    - 如果指定了 label prefix，Flink connector 将使用 label prefix 清理可能在某些 Flink 故障场景中生成的 lingering 事务，例如 Flink 作业在 checkpoint 仍在进行中时失败。如果您使用 `SHOW PROC '/transactions/<db_id>/running';` 在 StarRocks 中查看这些 lingering 事务，它们通常处于 `PREPARED` 状态。当 Flink 作业从 checkpoint 恢复时，Flink connector 将根据 label prefix 和 checkpoint 中的一些信息找到这些 lingering 事务，并中止它们。当 Flink 作业由于两阶段提交机制退出以实现 exactly-once 时，Flink connector 无法中止它们。当 Flink 作业退出时，Flink connector 尚未收到来自 Flink checkpoint coordinator 的通知，告知事务是否应包含在成功的 checkpoint 中，如果无论如何都中止这些事务，可能会导致数据丢失。您可以在这篇 [blogpost](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/) 中大致了解如何在 Flink 中实现端到端 exactly-once。

    - 如果未指定 label prefix，则 lingering 事务仅在超时后才会被 StarRocks 清理。但是，如果 Flink 作业在事务超时之前频繁失败，则正在运行的事务数可能会达到 StarRocks `max_running_txn_num_per_db` 的限制。您可以为 `PREPARED` 事务设置较小的超时，以便在未指定 label prefix 时更快地过期。请参阅以下关于如何设置 prepared 超时的内容。

- 如果您确定 Flink 作业在因停止或持续故障转移而导致长时间停机后最终会从 checkpoint 或 savepoint 恢复，请相应地调整以下 StarRocks 配置，以避免数据丢失。

  - 调整 `PREPARED` 事务超时。请参阅以下关于如何设置超时。

    超时需要大于 Flink 作业的停机时间。否则，包含在成功 checkpoint 中的 lingering 事务可能会在您重新启动 Flink 作业之前因超时而中止，从而导致数据丢失。

    请注意，当您为此配置设置较大的值时，最好指定 `sink.label-prefix` 的值，以便可以根据 label prefix 和 checkpoint 中的一些信息清理 lingering 事务，而不是由于超时（这可能会导致数据丢失）。

  - `label_keep_max_second` 和 `label_keep_max_num`：StarRocks FE 配置，默认值分别为 `259200` 和 `1000`。有关详细信息，请参见 [FE configurations](./loading_introduction/loading_considerations.md#fe-configurations)。`label_keep_max_second` 的值需要大于 Flink 作业的停机时间。否则，Flink connector 无法通过使用保存在 Flink 的 savepoint 或 checkpoint 中的事务标签来检查 StarRocks 中事务的状态，并确定这些事务是否已提交，这最终可能导致数据丢失。

- 如何设置 PREPARED 事务的超时

  - 对于 Connector 1.2.12+ 和 StarRocks 3.5.4+，您可以通过配置 connector 参数 `sink.properties.prepared_timeout` 来设置超时。默认情况下，该值未设置，它会回退到 StarRocks FE 的全局配置 `prepared_transaction_default_timeout_second`（默认值为 `86400`）。

  - 对于其他版本的 Connector 或 StarRocks，您可以通过配置 StarRocks FE 的全局配置 `prepared_transaction_default_timeout_second`（默认值为 `86400`）来设置超时。

### Flush 策略

Flink connector 会将数据缓存在内存中，并通过 Stream Load 将它们批量刷新到 StarRocks 中。至少一次 (at-least-once) 和精确一次 (exactly-once) 触发刷新的方式有所不同。

对于至少一次 (at-least-once)，当满足以下任一条件时，将触发刷新：

- 缓冲行的字节数达到上限 `sink.buffer-flush.max-bytes`
- 缓冲行的数量达到上限 `sink.buffer-flush.max-rows`。（仅对 sink version V1 有效）
- 自上次刷新以来经过的时间达到上限 `sink.buffer-flush.interval-ms`
- 触发 checkpoint

对于精确一次 (exactly-once)，仅当触发 checkpoint 时才会发生刷新。

### Merge Commit

Merge Commit 有助于扩展吞吐量，而不会成比例地增加 StarRocks 事务开销。如果没有 Merge Commit，每个 Flink sink 子任务都维护自己的 Stream Load 事务，因此增加 `sink.parallelism` 会导致更多的并发事务，并增加 StarRocks 上的 I/O 和 Compaction 成本。相反，保持较低的并行度会限制管道的整体容量。启用 Merge Commit 后，来自多个 sink 子任务的数据会在每个 Merge 窗口中合并到单个事务中。这允许您增加 `sink.parallelism` 以获得更高的吞吐量，而无需增加事务的数量。有关配置示例，请参见 [使用 merge commit 导入数据](#load-data-with-merge-commit)。

以下是使用 Merge Commit 时的一些重要注意事项：

- **单个并行度没有好处**

  如果 Flink sink 并行度为 1，则启用 Merge Commit 没有好处，因为只有一个子任务发送数据。由于服务器端的 Merge Commit 时间窗口，它甚至可能引入额外的延迟。

- **仅保证 at-least-once 语义**

  Merge Commit 仅保证 at-least-once 语义。它不支持 exactly-once 语义。启用 Merge Commit 后，请勿将 `sink.semantic` 设置为 `exactly-once`。

- **主键表的排序**

  默认情况下，`sink.merge-commit.max-concurrent-requests` 为 `Integer.MAX_VALUE`，这意味着单个 sink 子任务可能会并发发送多个 Stream Load 请求。这可能会导致乱序导入，这对于主键表来说可能存在问题。为了确保按顺序导入，请将 `sink.merge-commit.max-concurrent-requests` 设置为 `0`，但这会降低吞吐量。或者，您可以使用条件更新来防止较新的数据被较旧的数据覆盖。有关配置示例，请参见 [主键表的顺序导入](#in-order-loading-for-primary-key-tables)。

- **端到端导入延迟**

  总导入延迟包括两个部分：
  - **Connector 批处理延迟**：由 `sink.buffer-flush.interval-ms` 和 `sink.merge-commit.chunk.size` 控制。当达到 chunk 大小限制或经过刷新间隔时，数据将从 connector 中刷新，以先到者为准。最大 connector 端延迟为 `sink.buffer-flush.interval-ms`。较小的 `sink.buffer-flush.interval-ms` 会降低 connector 端延迟，但会以较小的批次发送数据。
  - **StarRocks merge 窗口**：由 `sink.properties.merge_commit_interval_ms` 控制。系统会等待此持续时间，以将来自多个子任务的请求合并到单个事务中。较大的值会提高合并效率（更多请求将合并到一个事务中），但会增加服务器端延迟。
  - 作为一般准则，请将 `sink.buffer-flush.interval-ms` 设置为小于或等于 `sink.properties.merge_commit_interval_ms`，以便每个子任务可以在每个 Merge 窗口中至少刷新一次。例如，如果 `merge_commit_interval_ms` 为 `10000`（10 秒），则可以将 `sink.buffer-flush.interval-ms` 设置为 `5000`（5 秒）或更短。

- **调整 `sink.parallelism` 和 `sink.properties.merge_commit_parallel`**

  这两个参数控制不同层的并行度，应独立调整：
  - `sink.parallelism` 控制 Flink sink 子任务的数量。每个子任务缓冲数据并将其发送到 StarRocks。当 Flink sink operator 受到 CPU 或内存限制时，增加此值——您可以监视 Flink 的每个 operator 的 CPU 和内存使用情况，以确定是否需要更多子任务。
  - `sink.properties.merge_commit_parallel` 控制 StarRocks 为每个 Merge Commit 事务创建的导入计划的并行度。当 StarRocks 成为瓶颈时，增加此值。您可以监视 StarRocks 指标 [merge_commit_pending_total](../administration/management/monitoring/metrics.md#merge_commit_pending_total)（待处理的 Merge Commit 任务数）和 [merge_commit_pending_bytes](../administration/management/monitoring/metrics.md#merge_commit_pending_bytes)（待处理任务持有的字节数），以确定是否需要在 StarRocks 端增加并行度——持续的高值表示导入计划无法跟上输入数据。

- **`sink.merge-commit.chunk.size` 和 `sink.buffer-flush.max-bytes` 之间的关系**：
  - `sink.merge-commit.chunk.size` 控制每个 Stream Load 请求（每个 chunk）的最大数据大小。当 chunk 中的数据达到此大小时，将立即刷新。
  - `sink.buffer-flush.max-bytes` 控制所有表的缓存数据的总内存限制。当总缓存数据超过此限制时，connector 将提前驱逐 chunk 以释放内存。
  - 因此，应将 `sink.buffer-flush.max-bytes` 设置为大于 `sink.merge-commit.chunk.size`，以允许累积至少一个完整的 chunk。通常，`sink.buffer-flush.max-bytes` 应比 `sink.merge-commit.chunk.size` 大几倍，尤其是在有多个表或高并发的情况下。

### 监控导入指标

Flink connector 提供了以下指标来监控数据导入。

| 指标名称                   | 类型    | 描述                                                              |
|--------------------------|---------|-------------------------------------------------------------------|
| totalFlushBytes          | counter | 成功写入的字节数。                                                    |
| totalFlushRows           | counter | 成功写入的行数。                                                      |
| totalFlushSucceededTimes | counter | 数据成功写入的次数。                                                  |
| totalFlushFailedTimes    | counter | 数据写入失败的次数。                                                  |
| totalFilteredRows        | counter | 被过滤的行数，包含在 totalFlushRows 中。                                |

## 示例

以下示例展示了如何使用 Flink Connector 通过 Flink SQL 或 Flink DataStream 将数据导入到 StarRocks 表中。

### 准备工作

#### 创建 StarRocks 表

创建一个名为 `test` 的数据库，并创建一个名为 `score_board` 的主键表。

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

#### 搭建 Flink 环境

- 下载 Flink 二进制文件 [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz) ，并将其解压缩到目录 `flink-1.15.2` 。
- 下载 [Flink connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar) ，并将其放入目录 `flink-1.15.2/lib` 。
- 运行以下命令以启动 Flink 集群：

```shell
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

#### 网络配置

确保 Flink 所在的机器可以通过 [`http_port`](../administration/management/FE_configuration.md#http_port) (默认：`8030`) 和 [`query_port`](../administration/management/FE_configuration.md#query_port) (默认：`9030`) 访问 StarRocks 集群的 FE 节点，并通过 [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) (默认：`8040`) 访问 BE 节点。

### 使用 Flink SQL 运行

- 运行以下命令来启动 Flink SQL 客户端。

```shell
    ./bin/sql-client.sh
    ```

- 创建一个 Flink 表 `score_board`，并通过 Flink SQL Client 将值插入到该表中。
  请注意，如果要将数据导入到 StarRocks 的主键表，则必须在 Flink DDL 中定义主键。对于其他类型的 StarRocks 表，这是可选的。

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

### 通过 Flink DataStream 运行

根据输入记录的类型，有几种方法可以实现 Flink DataStream 作业，例如 CSV Java `String`、JSON Java `String` 或自定义 Java 对象。

- 输入记录是 CSV 格式的 `String`。有关完整示例，请参见 [LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) 。

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

- 输入记录是 JSON 格式的 `String`。有关完整示例，请参见 [LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) 。

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

- 输入记录是自定义的 Java 对象。有关完整示例，请参见 [LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream) 。

  - 在此示例中，输入记录是一个简单的 POJO `RowData`。

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

- 主要程序如下：

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

- 主程序中的 `RowDataTransformer` 定义如下：

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

### 使用 Flink CDC 3.0 同步数据（支持 schema change）

[Flink CDC 3.0](https://nightlies.apache.org/flink/flink-cdc-docs-stable) 框架可以用于轻松构建从 CDC 源（例如 MySQL 和 Kafka）到 StarRocks 的流式 ELT pipeline。该 pipeline 可以同步整个数据库、合并分片表以及将 schema change 从源同步到 StarRocks。

从 v1.2.9 开始，StarRocks 的 Flink connector 已集成到此框架中，作为 [StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/pipeline-connectors/starrocks/)。 StarRocks Pipeline Connector 支持：

- 自动创建数据库和表
- Schema change 同步
- 全量和增量数据同步

有关快速入门，请参见 [使用带有 StarRocks Pipeline Connector 的 Flink CDC 3.0 从 MySQL 到 StarRocks 的流式 ELT](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/quickstart/mysql-to-starrocks) 。

建议使用 StarRocks v3.2.1 及更高版本，以启用 [fast_schema_evolution](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#set-fast-schema-evolution) 。这将提高添加或删除列的速度并减少资源使用。

## 最佳实践

### 导入数据到主键表

本节将展示如何将数据导入到 StarRocks 主键表，以实现部分更新和条件更新。
您可以参考 [通过导入更新数据](./Load_to_Primary_Key_tables.md) 了解这些功能的介绍。
这些示例使用 Flink SQL。

#### 准备工作

在 StarRocks 中创建数据库 `test` 和主键表 `score_board`。

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

#### 部分列更新

以下示例展示了如何仅将数据导入到 `id` 和 `name` 列。

1. 在 MySQL 客户端中，将两行数据插入到 StarRocks 表 `score_board` 中。

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

2. 在 Flink SQL 客户端中创建 Flink 表 `score_board`。

   - 定义仅包含 `id` 和 `name` 列的 DDL。
   - 将选项 `sink.properties.partial_update` 设置为 `true`，这会告知 Flink 连接器执行部分列更新。
   - 如果 Flink 连接器版本 `&lt;=` 1.2.7，您还需要将选项 `sink.properties.columns` 设置为 `id,name,__op`，以告知 Flink 连接器需要更新哪些列。请注意，您需要在末尾附加字段 `__op`。字段 `__op` 指示数据导入是 UPSERT 还是 DELETE 操作，其值由连接器自动设置。

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

3. 向 Flink 表中插入两条数据行。数据行的主键与 StarRocks 表中行的主键相同，但 `name` 列中的值已修改。

```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'flink-update');
    ```

4. 在 MySQL 客户端中查询 StarRocks 表。

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

您可以看到只有 `name` 的值发生了变化，而 `score` 的值没有变化。

#### 条件更新

以下示例展示了如何根据列 `score` 的值进行条件更新。仅当 `score` 的新值大于或等于旧值时，对 `id` 的更新才会生效。

1. 在 MySQL 客户端中，向 StarRocks 表中插入两行数据。

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

2. 通过以下方式创建 Flink 表 `score_board`：

    - 定义包含所有列的 DDL。
    - 将选项 `sink.properties.merge_condition` 设置为 `score`，以告知连接器使用 `score` 列作为条件。
    - 将选项 `sink.version` 设置为 `V1`，以告知连接器使用 Stream Load。

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

3. 向 Flink 表中插入两条数据行。数据行的主键与 StarRocks 表中数据行的主键相同。第一条数据行在 `score` 列中具有较小的值，第二条数据行在 `score` 列中具有较大的值。

```SQL
    INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'flink-update', 101);
    ```

4. 在 MySQL 客户端中查询 StarRocks 表。

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

您可以看到，只有第二行数据的值发生了变化，而第一行数据的值没有发生变化。

### 使用 Merge Commit 导入数据

本节介绍当您有多个 Flink sink 子任务写入同一个 StarRocks 表时，如何使用 Merge Commit 来提高数据导入吞吐量。以下示例使用 Flink SQL 和 StarRocks v3.4.0 或更高版本。

#### 准备工作

在 StarRocks 中创建数据库 `test`，并在该数据库中创建主键表 `score_board`。

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

#### 基本配置

此 Flink SQL 语句开启了 merge commit，合并窗口为 10 秒。来自所有 Sink 子任务的数据在每个窗口中合并到一个事务中。

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

将数据插入到 Flink 表中。这些数据将通过合并提交的方式加载到 StarRocks 中。

```SQL
INSERT INTO `score_board` VALUES (1, 'starrocks', 100), (2, 'flink', 95), (3, 'spark', 90);
```

#### 主键表的顺序导入

默认情况下，单个 Sink 子任务可能会并发发送多个 Stream Load 请求，这可能会导致乱序导入。对于数据顺序很重要的主键表，有两种方法可以解决此问题。

**方法 1：使用 `sink.merge-commit.max-concurrent-requests`**

将 `sink.merge-commit.max-concurrent-requests` 设置为 `0`，以确保每个子任务一次发送一个请求。这可以保证顺序导入，但可能会降低吞吐量。

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

**方法 2：使用条件更新**

如果您希望保持并发请求以获得更高的吞吐量，但仍要防止旧数据覆盖新数据，则可以使用[条件更新](#conditional-update) 。将 `sink.properties.merge_condition` 设置为某一列（例如，版本列或时间戳列），以便仅当传入值大于或等于现有值时，更新才会生效。

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

通过此配置，允许并发请求（默认 `sink.merge-commit.max-concurrent-requests` 为 `Integer.MAX_VALUE`），但仅当新的 `score` 大于或等于现有 `score` 时，对行的更新才会生效。 这样可以防止较新的数据被乱序导入的较旧数据覆盖。

### 导入数据至 BITMAP 类型的列

[`BITMAP`](../sql-reference/data-types/other-data-types/BITMAP.md) 常用于加速去重计数，例如统计 UV，请参见 [使用 Bitmap 实现精准去重](../using_starrocks/distinct_values/Using_bitmap.md)。
本文以统计 UV 为例，介绍如何将数据导入至 `BITMAP` 类型的列。

1. 在 MySQL 客户端中创建 StarRocks 聚合表。

   在数据库 `test` 中，创建一个聚合表 `page_uv`，其中 `visit_users` 列定义为 `BITMAP` 类型，并配置聚合函数 `BITMAP_UNION`。

```SQL
    CREATE TABLE `test`.`page_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. 在 Flink SQL 客户端中创建 Flink 表。

    Flink 表中的 `visit_user_id` 列是 `BIGINT` 类型，我们希望将此列导入到 StarRocks 表中 `BITMAP` 类型的 `visit_users` 列。因此，在定义 Flink 表的 DDL 时，请注意：
    - 因为 Flink 不支持 `BITMAP`，所以您需要定义一个 `visit_user_id` 列作为 `BIGINT` 类型，以表示 StarRocks 表中 `BITMAP` 类型的 `visit_users` 列。
    - 您需要设置选项 `sink.properties.columns` 为 `page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)`，这会告诉连接器 Flink 表和 StarRocks 表之间的列映射关系。此外，您需要使用 [`to_bitmap`](../sql-reference/sql-functions/bitmap-functions/to_bitmap.md) 函数来告诉连接器将 `BIGINT` 类型的数据转换为 `BITMAP` 类型。

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

3. 在 Flink SQL 客户端中将数据导入到 Flink 表。

```SQL
    INSERT INTO `page_uv` VALUES
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
       (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
       (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
       (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
    ```

4. 在 MySQL 客户端中，从 StarRocks 表计算页面 UV。

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

### 导入数据至 HLL 类型的列

[`HLL`](../sql-reference/data-types/other-data-types/HLL.md) 可用于近似去重，请参见[使用 HLL 进行近似去重](../using_starrocks/distinct_values/Using_HLL.md)。

本文以统计 UV 为例，介绍如何将数据导入至 `HLL` 类型的列。

1. 创建 StarRocks 聚合表

   在数据库 `test` 中，创建一张聚合表 `hll_uv`，其中 `visit_users` 列定义为 `HLL` 类型，并配置聚合函数 `HLL_UNION`。

```SQL
    CREATE TABLE `hll_uv` (
      `page_id` INT NOT NULL COMMENT 'page ID',
      `visit_date` datetime NOT NULL COMMENT 'access time',
      `visit_users` HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(`page_id`, `visit_date`)
    DISTRIBUTED BY HASH(`page_id`);
    ```

2. 在 Flink SQL 客户端中创建 Flink 表。

   Flink 表中的 `visit_user_id` 列是 `BIGINT` 类型，我们希望将此列加载到 StarRocks 表中 `HLL` 类型的 `visit_users` 列。因此，在定义 Flink 表的 DDL 时，请注意：
    - 因为 Flink 不支持 `BITMAP`，所以您需要定义一个 `visit_user_id` 列作为 `BIGINT` 类型，以表示 StarRocks 表中 `HLL` 类型的 `visit_users` 列。
    - 您需要设置选项 `sink.properties.columns` 为 `page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)`，这会告诉连接器 Flink 表和 StarRocks 表之间的列映射。此外，您需要使用 [`hll_hash`](../sql-reference/sql-functions/scalar-functions/hll_hash.md) 函数来告诉连接器将 `BIGINT` 类型的数据转换为 `HLL` 类型。

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

3. 在 Flink SQL 客户端中将数据导入到 Flink 表。

```SQL
    INSERT INTO `hll_uv` VALUES
       (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
       (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
       (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. 在 MySQL 客户端中，从 StarRocks 表计算页面 UV。

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