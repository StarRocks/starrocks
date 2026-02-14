---
displayed_sidebar: docs
---

# 从 Apache Flink® 持续导入

StarRocks 提供 Apache Flink® 连接器 (以下简称 Flink connector)，可以通过 Flink 导入数据至 StarRocks表。

基本原理是 Flink connector 在内存中积攒小批数据，再通过 [Stream Load](./StreamLoad.md) 一次性导入 StarRocks。

Flink Connector 支持 DataStream API，Table API & SQL 和 Python API。

StarRocks 提供的 Flink connector，相比于 Flink 提供的 [flink-connector-jdbc](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/jdbc/)，性能更优越和稳定。

> **注意**
>
> 使用 Flink connector 导入数据至 StarRocks 需要目标表的 SELECT 和 INSERT 权限。如果您的用户账号没有这些权限，请参考 [GRANT](../sql-reference/sql-statements/account-management/GRANT.md) 给用户赋权。

## 版本要求

| Connector | Flink                         | StarRocks     | Java | Scala     |
|-----------|-------------------------------|---------------| ---- |-----------|
| 1.2.14    | 1.16,1.17,1.18,1.19,1.20      | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.12    | 1.16,1.17,1.18,1.19,1.20      | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.11    | 1.15,1.16,1.17,1.18,1.19,1.20 | 2.1 及更高版本 | 8    | 2.11,2.12 |
| 1.2.10    | 1.15,1.16,1.17,1.18,1.19      | 2.1 及更高版本 | 8    | 2.11,2.12 |

## 获取 Flink connector

您可以通过以下方式获取 Flink connector JAR 文件：

- 直接下载已经编译好的 JAR 文件。
- 在 Maven 项目的 pom 文件添加 Flink connector 为依赖项，作为依赖下载。
- 通过源码手动编译成 JAR 文件。

Flink connector JAR 文件的命名格式如下：

- 适用于 Flink 1.15 版本及以后的 Flink connector 命名格式为 `flink-connector-starrocks-${connector_version}_flink-${flink_version}.jar`。例如您安装了 Flink 1.15，并且想要使用 1.2.7 版本的 Flink connector，则您可以使用 `flink-connector-starrocks-1.2.7_flink-1.15.jar`。
- 适用于 Flink 1.15 版本之前的 Flink connector 命名格式为 `flink-connector-starrocks-${connector_version}_flink-${flink_version}_${scala_version}.jar`。例如您安装了 Flink 1.14 和 Scala 2.12，并且您想要使用 1.2.7 版本的 Flink connector，您可以使用 `flink-connector-starrocks-1.2.7_flink-1.14_2.12.jar`。

> **注意**
>
> 一般情况下最新版本的 Flink connector 只维护最近 3 个版本的 Flink。

### 直接下载

可以在 [Maven Central Repository](https://repo1.maven.org/maven2/com/starrocks) 获取不同版本的 Flink connector JAR 文件。

### Maven 依赖

在 Maven 项目的 `pom.xml` 文件中，根据以下格式将 Flink connector 添加为依赖项。将 `flink_version`、`scala_version` 和 `connector_version` 分别替换为相应的版本。

- 适用于 Flink 1.15 版本及以后的 Flink connector

    ```XML
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}</version>
    </dependency>
    ```

- 适用于 Flink 1.15 版本之前的 Flink connector

    ```XML
    <dependency>
        <groupId>com.starrocks</groupId>
        <artifactId>flink-connector-starrocks</artifactId>
        <version>${connector_version}_flink-${flink_version}_${scala_version}</version>
    </dependency>
    ```

### 手动编译

1. 下载 [Flink connector 代码](https://github.com/StarRocks/starrocks-connector-for-apache-flink)。
2. 执行以下命令将 Flink connector 的源代码编译成一个 JAR 文件。请注意，将 `flink_version` 替换为相应的Flink 版本。

    ```Bash
    sh build.sh <flink_version>
    ```

    例如，如果您的环境中的 Flink 版本为1.15，您需要执行以下命令：

    ```Bash
    sh build.sh 1.15
    ```

3. 前往 `target/` 目录，找到编译完成的 Flink connector JAR 文件，例如 `flink-connector-starrocks-1.2.7_flink-1.15-SNAPSHOT.jar`，该文件在编译过程中生成。

    > **注意**：
    >
    > 未正式发布的 Flink connector 的名称包含 `SNAPSHOT` 后缀。

## 参数说明

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

## 数据类型映射

| Flink 数据类型                    | StarRocks 数据类型 |
| --------------------------------- | ------------------ |
| BOOLEAN                           | BOOLEAN            |
| TINYINT                           | TINYINT            |
| SMALLINT                          | SMALLINT           |
| INTEGER                           | INTEGER            |
| BIGINT                            | BIGINT             |
| FLOAT                             | FLOAT              |
| DOUBLE                            | DOUBLE             |
| DECIMAL                           | DECIMAL            |
| BINARY                            | INT                |
| CHAR                              | STRING             |
| VARCHAR                           | STRING             |
| STRING                            | STRING             |
| DATE                              | DATE               |
| TIMESTAMP_WITHOUT_TIME_ZONE(N)    | DATETIME           |
| TIMESTAMP_WITH_LOCAL_TIME_ZONE(N) | DATETIME           |
| ARRAY&lt;T&gt;                    | ARRAY&lt;T&gt;     |
| MAP&lt;KT,VT&gt;                  | JSON STRING        |
| ROW&lt;arg T...&gt;               | JSON STRING        |

## 使用说明

### Exactly Once

- 如果您希望 sink 保证 exactly-once 语义，则建议升级 StarRocks 到 2.5 或更高版本，并将 Flink connector 升级到 1.2.4 或更高版本。

  - 自 2.4 版本 StarRocks 开始支持 [Stream Load 事务接口](./Stream_Load_transaction_interface.md)。自 Flink connector 1.2.4 版本起， Sink 基于 Stream Load 事务接口重新设计 exactly-once 的实现，相较于原来基于 Stream Load 非事务接口实现的 exactly-once，降低了内存使用和 checkpoint 耗时，提高了作业的实时性和稳定性。
  - 自 Flink connector 1.2.4 版本起，如果 StarRocks 支持 Stream Load 事务接口，则 Sink 默认使用 Stream Load 事务接口，如果需要使用 Stream Load  非事务接口实现，则需要配置 `sink.version` 为`V1`。
  > **注意**
  >
  > 如果只升级 StarRocks 或 Flink connector，sink 会自动选择  Stream Load  非事务接口实现。

- sink 保证 exactly-once 语义相关配置
  
  - `sink.semantic` 的值必须为 `exactly-once`.
  
  - 如果 Flink connector 版本为 1.2.8 及更高，则建议指定 `sink.label-prefix` 的值。需要注意的是，label 前缀在 StarRocks 的所有类型的导入作业中必须是唯一的，包括 Flink job、Routine Load 和 Broker Load。

    - 如果指定了 label 前缀，Flink connector 将使用 label 前缀清理因为 Flink job 失败而生成的未完成事务，例如在checkpoint 进行过程中 Flink job 失败。如果使用 `SHOW PROC '/transactions/<db_id>/running';` 查看这些事务在 StarRock 的状态，则返回结果会显示事务通常处于 `PREPARED` 状态。当 Flink job 从 checkpoint 恢复时，Flink connector 将根据 label 前缀和 checkpoint 中的信息找到这些未完成的事务，并中止事务。当 Flink job 因某种原因退出时，由于采用了两阶段提交机制来实现 exactly-once语义，Flink connector 无法中止事务。当 Flink 作业退出时，Flink connector 尚未收到来自 Flink checkpoint coordinator 的通知，说明这些事务是否应包含在成功的 checkpoint 中，如果中止这些事务，则可能导致数据丢失。您可以在这篇[文章](https://flink.apache.org/2018/02/28/an-overview-of-end-to-end-exactly-once-processing-in-apache-flink-with-apache-kafka-too/)中了解如何在 Flink 中实现端到端的 exactly-once。

    - 若未指定 label 前缀，StarRocks 仅会在超时后清理滞留事务。但若 Flink 作业在事务超时前频繁失败，运行中的事务数量可能达到 StarRocks `max_running_txn_num_per_db` 的限制。当标签前缀未指定时，可为 `PREPARED` 事务设置更短的超时时间使其更快失效。关于预备状态超时设置方法，请参阅以下说明。

- 如果您确定 Flink job 将在长时间停止后最终会使用 checkpoint 或 savepoint 恢复，则为避免数据丢失，请调整以下 StarRocks 配置：

  - 调整 `PREPARED` 事务超时。关于如何设置超时，请参阅以下说明。

    该超时时间需大于 Flink 作业的停机时间。否则，在重启 Flink 作业前，包含在成功 checkpoint 中的滞留事务可能因超时而被中止，导致数据丢失。

    请注意：当您将此配置值设为较大数值时，建议同时指定 `sink.label-prefix` 的值，以便根据标签前缀和检查点中的信息清理滞留事务，而非依赖超时机制（后者可能导致数据丢失）。

  - `label_keep_max_second` 和 `label_keep_max_num`：StarRocks FE 参数，默认值分别为 `259200` 和 `1000`。更多信息，参见[FE 配置](./loading_introduction/loading_considerations.md#fe-配置)。`label_keep_max_second` 的值需要大于 Flink job 的停止时间。否则，Flink connector 无法使用保存在 Flink 的 savepoint 或 checkpoint 中的事务 label 来检查事务在 StarRocks 中的状态，并判断这些事务是否已提交，最终可能导致数据丢失。

- 如何设置 `PREPARED` 事务的超时时间

  - 对于 Connector 1.2.12+ 和 StarRocks 3.5.4+，可通过配置连接器参数 `sink.properties.prepared_timeout` 设置超时值。默认情况下该值未设置，此时将回退至 StarRocks FE 的全局配置 `prepared_transaction_default_timeout_second`（默认值为 `86400`）。

  - 对于其他版本的连接器或 StarRocks，可通过配置 StarRocks FE 的全局配置项 `prepared_transaction_default_timeout_second`（默认值为 `86400`）来设置超时。

### Flush 策略

Flink connector 先在内存中 buffer 数据，然后通过 Stream Load 将其一次性 flush 到 StarRocks。在 at-least-once 和 exactly-once 场景中使用不同的方式触发 flush 。

对于 at-least-once，在满足以下任何条件时触发 flush：

- buffer 数据的字节达到限制 `sink.buffer-flush.max-bytes`
- buffer 数据行数达到限制 `sink.buffer-flush.max-rows`。（仅适用于版本 V1）
- 自上次 flush 以来经过的时间达到限制 `sink.buffer-flush.interval-ms`
- 触发了 checkpoint

对于 exactly-once，仅在触发 checkpoint 时触发 flush。

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

Flink connector 提供以下指标来监控导入情况。

| 指标名称                     | 类型   | 描述                                               |
| ------------------------ | ------ | -------------------------------------------------- |
| totalFlushBytes          | Counter| 成功 flush 的字节。                                 |
| totalFlushRows           | Counter | 成功 flush 的行数。                                   |
| totalFlushSucceededTimes | Counter |  flush 数据的成功次数。                           |
| totalFlushFailedTimes    | Counter | flush 数据的失败次数。                                   |
| totalFilteredRows        | Counter | 已过滤的行数，这些行数也包含在 totalFlushRows 中。 |

## 使用示例

### 准备工作

#### 创建 StarRocks 表

创建数据库 `test`，并创建主键表  `score_board`。

```SQL
CREATE DATABASE test;

CREATE TABLE test.score_board(
    id int(11) NOT NULL COMMENT "",
    name varchar(65533) NULL DEFAULT "" COMMENT "",
    score int(11) NOT NULL DEFAULT "0" COMMENT ""
)
ENGINE=OLAP
PRIMARY KEY(id)
DISTRIBUTED BY HASH(id);
```

#### Flink 环境

- 下载 Flink 二进制文件 [Flink 1.15.2](https://archive.apache.org/dist/flink/flink-1.15.2/flink-1.15.2-bin-scala_2.12.tgz)，并解压到目录 `flink-1.15.2`。
- 下载 [Flink connector 1.2.7](https://repo1.maven.org/maven2/com/starrocks/flink-connector-starrocks/1.2.7_flink-1.15/flink-connector-starrocks-1.2.7_flink-1.15.jar)，并将其放置在目录 `flink-1.15.2/lib` 中。
- 运行以下命令启动 Flink 集群：

    ```Bash
    cd flink-1.15.2
    ./bin/start-cluster.sh
    ```

#### 网络配置

确保 Flink 所在机器能够访问 StarRocks 集群中 FE 节点的 [`http_port`](../administration/management/FE_configuration.md#http_port)（默认 `8030`） 和 [`query_port`](../administration/management/FE_configuration.md#query_port) 端口（默认 `9030`），以及 BE 节点的 [`be_http_port`](../administration/management/BE_configuration.md#be_http_port) 端口（默认 `8040`）。

### 使用 Flink SQL 写入数据

- 运行以下命令以启动 Flink SQL 客户端。

    ```Bash
    ./bin/sql-client.sh
    ```

- 在 Flink SQL 客户端，创建一个表 `score_board`，并且插入数据。 注意，如果您想将数据导入到 StarRocks 主键表中，您必须在 Flink 表的 DDL 中定义主键。对于其他类型的 StarRocks 表，这是可选的。

    ```sql
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

### 使用 Flink DataStream 写入数据

根据 input records 的类型，编写对应 Flink DataStream 作业，例如 input records 为 CSV 格式的 Java `String`、JSON 格式的 Java `String` 或自定义的 Java 对象。

- 如果 input records 为 CSV 格式的 `String`，对应的 Flink DataStream 作业的主要代码如下所示，完整代码请参见 [LoadCsvRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream)

    ```Java
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
     * Configure the Flink connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.column_separator"
     * to tell the Flink connector the input records are CSV-format, and the column separator is "\t".
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

- 如果 input records 为 JSON 格式的 `String`，对应的 Flink DataStream 作业的主要代码如下所示，完整代码请参见[LoadJsonRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream)

    ```Java
    /**
     * Generate JSON-format records. 
     * Each record has three key-value pairs corresponding to the columns id, name, and score in the StarRocks table.
     */
    String[] records = new String[]{
            "{\"id\":1, \"name\":\"starrocks-json\", \"score\":100}",
            "{\"id\":2, \"name\":\"flink-json\", \"score\":100}",
    };
    DataStream<String> source = env.fromElements(records);
    
    /** 
     * Configure the Flink connector with the required properties.
     * You also need to add properties "sink.properties.format" and "sink.properties.strip_outer_array"
     * to tell the Flink connector the input records are JSON-format and to strip the outermost array structure. 
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

- 如果 input records 为自定义的 Java 对象，对应的 Flink DataStream 作业的主要代码如下所示，完整代码请参见[LoadCustomJavaRecords](https://github.com/StarRocks/starrocks-connector-for-apache-flink/tree/cd8086cfedc64d5181785bdf5e89a847dc294c1d/examples/src/main/java/com/starrocks/connector/flink/examples/datastream)

  - 本示例中，input record 是一个简单的 POJO `RowData`。

    ```Java
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

  - 主要代码如下所示：

    ```Java
    // Generate records which use RowData as the container.
    RowData[] records = new RowData[]{
            new RowData(1, "starrocks-rowdata", 100),
            new RowData(2, "flink-rowdata", 100),
        };
    DataStream<RowData> source = env.fromElements(records);
    
    // Configure the Flink connector with the required properties.
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

  - 其中 `RowDataTransformer` 定义如下：

    ```Java
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

[Flink CDC 3.0 框架](https://nightlies.apache.org/flink/flink-cdc-docs-stable)可以轻松地从 CDC 数据源（如 MySQL、Kafka）到 StarRocks 构建流式 ELT 管道。该管道能够将整个数据库、分库分表以及来自源端的 schema change 同步到 StarRocks。

自 v1.2.9 起，StarRocks 提供的 Flink connector 已经集成至该框架中，并且被命名为 [StarRocks Pipeline Connector](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/pipeline-connectors/starrocks/)。StarRocks Pipeline Connector 支持：

- 自动创建数据库/表
- 同步 schema change
- 同步全量和增量数据

快速上手教程可以参考[从 MySQL 到 StarRocks 的流式 ELT 管道](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/get-started/quickstart/mysql-to-starrocks)。

建议您使用 StarRocks v3.2.1 及以后的版本，以开启 [fast_schema_evolution](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md#设置-fast-schema-evolution)，来提高加减列的速度并降低资源使用。

## 最佳实践

### 导入至主键表

本节将展示如何将数据导入到 StarRocks 主键表中，以实现部分更新和条件更新。以下示例使用 Flink SQL。 部分更新和条件更新的更多介绍，请参见[通过导入实现数据变更](./Load_to_Primary_Key_tables.md)。

#### 准备工作

在StarRocks中创建一个名为`test`的数据库，并在其中创建一个名为`score_board`的主键表。

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

#### 部分更新

本示例展示如何通过导入数据仅更新 StarRocks 表中列 `name`的值。

1. 在 MySQL 客户端向 StarRocks 表 `score_board` 插入两行数据。

      ```sql
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

2. 在 Flink SQL 客户端创建表 `score_board` 。
   - DDL 中仅包含列 `id` 和 `name` 的定义。
   - 将选项 `sink.properties.partial_update` 设置为 `true`，以要求 Flink connector 执行部分更新。
   - 如果 Flink connector 版本小于等于 1.2.7，则还需要将选项 `sink.properties.columns` 设置为`id,name,__op`，以告诉 Flink connector 需要更新的列。请注意，您需要在末尾附加字段 `__op`。字段 `__op` 表示导入是 UPSERT 还是 DELETE 操作，其值由 Flink connector 自动设置。

      ```sql
      CREATE TABLE score_board (
          id INT,
          name STRING,
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

3. 将两行数据插入两行数据到表中。数据行的主键与 StarRocks 表的数据行主键相同，但是 `name` 列的值被修改。

      ```SQL
      INSERT INTO `score_board` VALUES (1, 'starrocks-update'), (2, 'flink-update');
      ```

4. 在 MySQL 客户端查询 StarRocks 表。

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

    您会看到只有 `name` 列的值发生了变化，而 `score` 列的值没有变化。

#### 条件更新

本示例展示如何根据 `score` 列的值进行条件更新。只有导入的数据行中 `score` 列值大于等于 StarRocks 表当前值时，该数据行才会更新。

1. 在 MySQL 客户端中向 StarRocks 表中插入两行数据。

   ```sql
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

2. 在 Flink SQL 客户端按照以下方式创建表`score_board`：
   - DDL 中包括所有列的定义。
   - 将选项  `sink.properties.merge_condition` 设置为 `score`，要求 Flink connector 使用 `score`  列作为更新条件。
   - 将选项 `sink.version` 设置为 `V1` ，要求 Flink connector 使用 Stream Load 接口导入数据。因为只有 Stream Load 接口支持条件更新。

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

3. 在 Flink SQL 客户端插入两行数据到表中。数据行的主键与 StarRocks 表中的行相同。第一行数据 `score` 列中具有较小的值，而第二行数据 `score` 列中具有较大的值。

      ```SQL
      INSERT INTO `score_board` VALUES (1, 'starrocks-update', 99), (2, 'flink-update', 101);
      ```

4. 在 MySQL客户端查询 StarRocks表。

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

    您会注意到仅第二行数据发生了变化，而第一行数据未发生变化。

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

### 导入至 Bitmap 列

`BITMAP` 常用于加速精确去重计数，例如计算独立访客数（UV），更多信息，请参见[使用 Bitmap 实现精确去重](../using_starrocks/distinct_values/Using_bitmap.md)。

本示例以计算独立访客数（UV）为例，展示如何导入数据至 StarRocks 表 `BITMAP` 列中。

1. 在 MySQL 客户端中创建一个 StarRocks 聚合表。

   在数据库`test`中，创建聚合表 `page_uv`，其中列 `visit_users` 被定义为 `BITMAP` 类型，并配置聚合函数 `BITMAP_UNION`。

      ```SQL
      CREATE TABLE `test`.`page_uv` (
        `page_id` INT NOT NULL COMMENT 'page ID',
        `visit_date` datetime NOT NULL COMMENT 'access time',
        `visit_users` BITMAP BITMAP_UNION NOT NULL COMMENT 'user ID'
      ) ENGINE=OLAP
      AGGREGATE KEY(`page_id`, `visit_date`)
      DISTRIBUTED BY HASH(`page_id`);
      ```

2. 在 Flink SQL 客户端中创建一个表。

   因为表中的 `visit_user_id` 列是`BIGINT`类型，我们希望将此列的数据导入到StarRocks表中的`visit_users`列，该列是`BITMAP`类型。因此，在定义表的 DDL 时，需要注意以下几点：

   - 由于 Flink 不支持 `BITMAP` 类型，您需要将 `visit_user_id` 列定义为`BIGINT`类型，以代表StarRocks表中的 `visit_users` 列。
   - 您需要将选项 `sink.properties.columns` 设置为`page_id,visit_date,user_id,visit_users=to_bitmap(visit_user_id)`，以告诉 Flink connector 如何将该表的列和 StarRocks 表的列进行映射，并且还需要使用 `to_bitmap` 函数，将`BIGINT` 类型 `visit_user_id` 列的数据转换为 `BITMAP`类型。

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

3. 在 Flink SQL 客户端中插入数据至表中。

      ```SQL
      INSERT INTO `page_uv` VALUES
         (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 13),
         (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23),
         (1, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 33),
         (1, CAST('2020-06-23 02:30:30' AS TIMESTAMP), 13),
         (2, CAST('2020-06-23 01:30:30' AS TIMESTAMP), 23);
      ```

4. 在 MySQL 客户端查询 StarRocks 表来计算页面 UV 数。

      ```SQL
      MySQL [test]> SELECT page_id, COUNT(DISTINCT visit_users) FROM page_uv GROUP BY page_id;
      +---------+-----------------------------+
      +---------+-----------------------------+
      +---------+-----------------------------+
      2 rows in set (0.05 sec)
      ```

### 导入至 HLL 列

`HLL` 可用于近似去重计数，更多信息，请参见[使用 HLL 实现近似去重](../using_starrocks/distinct_values/Using_HLL.md)。

本示例以计算独立访客数（UV）为例，展示如何导入数据至 StarRocks 表 `HLL` 列中。

1. 在 MySQL 客户端中创建一个 StarRocks 聚合表。

   在数据库 `test` 中，创建一个名为`hll_uv`的聚合表，其中列`visit_users`被定义为`HLL`类型，并配置聚合函数`HLL_UNION`。

    ```SQL
    CREATE TABLE hll_uv (
    page_id INT NOT NULL COMMENT 'page ID',
    visit_date datetime NOT NULL COMMENT 'access time',
    visit_users HLL HLL_UNION NOT NULL COMMENT 'user ID'
    ) ENGINE=OLAP
    AGGREGATE KEY(page_id, visit_date)
    DISTRIBUTED BY HASH(page_id);
    ```

2. 在 Flink SQL客户端中创建一个表。

   表中的`visit_user_id`列是`BIGINT`类型，我们希望将此列的数据导入至 StarRocks 表中的`visit_users`列，该列是 `HLL` 类型。因此，在定义表的 DDL 时，需要注意以下几点：

    - 由于 Flink 不支持`HLL`类型，您需要将 `visit_user_id` 列定义为 `BIGINT` 类型，以代表 StarRocks 表中的 `visit_users` 列。
    - 您需要将选项 `sink.properties.columns` 设置为`page_id,visit_date,user_id,visit_users=hll_hash(visit_user_id)`，以告诉 Flink connector 如何将该表的列和 StarRocks 表的列进行映射。还需要使用 `hll_hash` 函数，将 `BIGINT` 类型的 `visit_user_id` 列的数据转换为 `HLL` 类型。

    ```SQL
    CREATE TABLE hll_uv (
        page_id INT,
        visit_date TIMESTAMP,
        visit_user_id BIGINT
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

3. 在 Flink SQL 客户端中插入数据至表中。

    ```SQL
    INSERT INTO hll_uv VALUES
    (3, CAST('2023-07-24 12:00:00' AS TIMESTAMP), 78),
    (4, CAST('2023-07-24 13:20:10' AS TIMESTAMP), 2),
    (3, CAST('2023-07-24 12:30:00' AS TIMESTAMP), 674);
    ```

4. 在 MySQL 客户端查询 StarRocks 表来计算页面 UV 数。

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
