---
displayed_sidebar: docs
---

import Tip from '../../../../_assets/commonMarkdown/quickstart-routine-load-tip.mdx';
import WarehouseExample from '../../../../_assets/sql-reference/sql-statements/loading_unloading/routine_load/_routine_load_warehouse_example.mdx';
import WarehouseProperty from '../../../../_assets/sql-reference/sql-statements/loading_unloading/routine_load/_routine_load_warehouse_property.mdx';

# CREATE ROUTINE LOAD

<Tip />

Routine Load 可以持续从 Apache Kafka® 消费消息并将数据导入到 StarRocks。Routine Load 可以从 Kafka 集群中消费 CSV、JSON 和 Avro（自 v3.0.1 起支持）格式的数据，并通过多种安全协议访问 Kafka，包括 `plaintext`、`ssl`、`sasl_plaintext` 和 `sasl_ssl`。

本主题介绍 CREATE ROUTINE LOAD 语句的语法、参数和示例。

:::note
- 有关 Routine Load 的应用场景、原理和基本操作的信息，请参见 [使用 Routine Load 导入数据](../../../../loading/Loading_intro.md)。
- 只有具有 StarRocks 表 INSERT 权限的用户才能将数据导入到 StarRocks 表中。如果您没有 INSERT 权限，请按照 [GRANT](../../account-management/GRANT.md) 中提供的说明授予您用于连接 StarRocks 集群的用户 INSERT 权限。
:::

## 语法

```SQL
CREATE ROUTINE LOAD <database_name>.<job_name> ON <table_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## 参数

### `database_name`, `job_name`, `table_name`

`database_name`

可选。StarRocks 数据库的名称。

`job_name`

必需。Routine Load 作业的名称。一个表可以从多个 Routine Load 作业接收数据。我们建议您使用可识别的信息（例如 Kafka 主题名称和大致的作业创建时间）设置一个有意义的 Routine Load 作业名称，以区分多个 Routine Load 作业。Routine Load 作业的名称在同一数据库中必须唯一。

`table_name`

必需。数据导入的 StarRocks 表的名称。

### `load_properties`

可选。数据的属性。语法：

```SQL
[COLUMNS TERMINATED BY '<column_separator>'],
[ROWS TERMINATED BY '<row_separator>'],
[COLUMNS (<column1_name>[, <column2_name>, <column_assignment>, ... ])],
[WHERE <expr>],
[PARTITION (<partition1_name>[, <partition2_name>, ...])]
[TEMPORARY PARTITION (<temporary_partition1_name>[, <temporary_partition2_name>, ...])]
```

`COLUMNS TERMINATED BY`

CSV 格式数据的列分隔符。默认列分隔符是 `\t`（Tab）。例如，您可以使用 `COLUMNS TERMINATED BY ","` 将列分隔符指定为逗号。

:::tip
- 确保此处指定的列分隔符与要导入的数据中的列分隔符相同。
- 您可以使用 UTF-8 字符串（如逗号（,）、制表符或管道符号（|）），其长度不超过 50 字节，作为文本分隔符。
- 空值用 `\N` 表示。例如，一个数据记录由三列组成，数据记录在第一列和第三列中有数据，但在第二列中没有数据。在这种情况下，您需要在第二列中使用 `\N` 表示空值。这意味着记录必须编译为 `a,\N,b` 而不是 `a,,b`。`a,,b` 表示记录的第二列包含一个空字符串。
:::

`ROWS TERMINATED BY`

CSV 格式数据的行分隔符。默认行分隔符是 `\n`。

`COLUMNS`

源数据中的列与 StarRocks 表中的列之间的映射。有关更多信息，请参见本主题中的 [列映射](#column-mapping)。

- `column_name`：如果源数据中的某列可以直接映射到 StarRocks 表中的某列，则只需指定列名。这些列可以称为映射列。
- `column_assignment`：如果源数据中的某列不能直接映射到 StarRocks 表中的某列，并且该列的值必须在数据导入之前通过函数进行计算，则必须在 `expr` 中指定计算函数。这些列可以称为派生列。建议将派生列放在映射列之后，因为 StarRocks 首先解析映射列。

`WHERE`

过滤条件。只有满足过滤条件的数据才能导入到 StarRocks。例如，如果您只想导入 `col1` 值大于 `100` 且 `col2` 值等于 `1000` 的行，可以使用 `WHERE col1 > 100 and col2 = 1000`。

:::note
过滤条件中指定的列可以是源列或派生列。
:::

`PARTITION`

如果 StarRocks 表分布在分区 p0、p1、p2 和 p3 上，并且您只想将数据导入到 StarRocks 中的 p1、p2 和 p3，并过滤掉将存储在 p0 中的数据，则可以指定 `PARTITION(p1, p2, p3)` 作为过滤条件。默认情况下，如果您未指定此参数，数据将导入到所有分区。示例：

```SQL
PARTITION (p1, p2, p3)
```

`TEMPORARY PARTITION`

要导入数据的 [临时分区](../../../../table_design/data_distribution/Temporary_partition.md) 的名称。您可以指定多个临时分区，必须用逗号（,）分隔。

### `job_properties`

必需。导入作业的属性。语法：

```SQL
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```
<WarehouseProperty />

#### `desired_concurrent_number`

**必需**：否\
**描述**：单个 Routine Load 作业的期望任务并行度。默认值：`3`。实际任务并行度由多个参数的最小值决定：`min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)`。<ul><li>`alive_be_number`：存活的 BE 节点数。</li><li>`partition_number`：要消费的分区数。</li><li>`desired_concurrent_number`：单个 Routine Load 作业的期望任务并行度。默认值：`3`。</li><li>`max_routine_load_task_concurrent_num`：Routine Load 作业的默认最大任务并行度，为 `5`。请参见 [FE 动态参数](../../../../administration/management/FE_configuration.md#configure-fe-dynamic-parameters)。</li></ul>最大实际任务并行度由存活的 BE 节点数或要消费的分区数决定。<br/>

####  `max_batch_interval`

**必需**：否\
**描述**：任务的调度间隔，即任务执行的频率。单位：秒。取值范围：`5` ~ `60`。默认值：`10`。建议设置大于 `10` 的值。如果调度时间小于 10 秒，由于导入频率过高，会生成过多的 tablet 版本。

#### `max_batch_rows`

**必需**：否\
**描述**：此属性仅用于定义错误检测窗口。窗口是单个 Routine Load 任务消费的数据行数。值为 `10 * max_batch_rows`。默认值为 `10 * 200000 = 2000000`。Routine Load 任务在错误检测窗口中检测错误数据。错误数据是指 StarRocks 无法解析的数据，例如无效的 JSON 格式数据。

#### `max_error_number`

**必需**：否\
**描述**：错误检测窗口内允许的最大错误数据行数。如果错误数据行数超过此值，导入作业将暂停。您可以执行 [SHOW ROUTINE LOAD](SHOW_ROUTINE_LOAD.md) 并使用 `ErrorLogUrls` 查看错误日志。之后，您可以根据错误日志在 Kafka 中纠正错误。默认值为 `0`，表示不允许错误行。<br />**注意** <br /> <ul><li>当错误数据行数过多时，最后一批任务将在导入作业暂停之前**成功**。也就是说，合格的数据将被导入，不合格的数据将被过滤。如果您不想过滤太多不合格的数据行，请设置参数 `max_filter_ratio`。</li><li>错误数据行不包括 WHERE 子句过滤掉的数据行。</li><li>此参数与下一个参数 `max_filter_ratio` 一起控制最大错误数据记录数。当未设置 `max_filter_ratio` 时，此参数的值生效。当设置了 `max_filter_ratio` 时，一旦错误数据记录数达到此参数或 `max_filter_ratio` 参数设置的阈值，导入作业将暂停。</li></ul>

#### `max_filter_ratio`

**必需**：否\
**描述**：导入作业的最大错误容忍度。错误容忍度是指由于数据质量不足而被过滤掉的数据记录在所有请求导入作业的数据记录中的最大百分比。有效值：`0` 到 `1`。默认值：`1`（这意味着它实际上不会生效）。<br/>建议您将其设置为 `0`。这样，如果检测到不合格的数据记录，导入作业将暂停，从而确保数据的正确性。<br/>如果您想忽略不合格的数据记录，可以将此参数设置为大于 `0` 的值。这样，即使数据文件包含不合格的数据记录，导入作业也可以成功。<br/>**注意**<br/><ul><li>当错误数据行数大于 `max_filter_ratio` 时，最后一批任务将**失败**。这与 `max_error_number` 的效果有些**不同**。</li><li>不合格的数据记录不包括 WHERE 子句过滤掉的数据记录。</li><li>此参数与上一个参数 `max_error_number` 一起控制最大错误数据记录数。当未设置此参数（与设置 `max_filter_ratio = 1` 的效果相同）时，`max_error_number` 参数的值生效。当设置了此参数时，一旦错误数据记录数达到此参数或 `max_error_number` 参数设置的阈值，导入作业将暂停。</li></ul>

#### `strict_mode`      

**必需**：否\
**描述**：指定是否启用 [strict mode](../../../../loading/load_concept/strict_mode.md)。有效值：`true` 和 `false`。默认值：`false`。当启用严格模式时，如果导入数据中某列的值为 `NULL`，但目标表不允许该列的 `NULL` 值，则该数据行将被过滤掉。

#### `log_rejected_record_num`

**必需**：否\
**描述**：指定可以记录的最大不合格数据行数。此参数自 v3.1 起支持。有效值：`0`、`-1` 和任何非零正整数。默认值：`0`。<ul><li>值 `0` 指定被过滤掉的数据行不会被记录。</li><li>值 `-1` 指定所有被过滤掉的数据行都会被记录。</li><li>非零正整数如 `n` 指定每个 BE 上最多可以记录 `n` 个被过滤掉的数据行。</li></ul>您可以通过查询 `information_schema.loads` 视图中的 `REJECTED_RECORD_PATH` 字段返回的路径访问导入作业中被过滤掉的所有不合格数据行。

#### `timezone`    

**必需**：否\
**描述**：导入作业使用的时区。默认值：`Asia/Shanghai`。此参数的值会影响函数如 strftime()、alignment_timestamp() 和 from_unixtime() 返回的结果。此参数指定的时区是会话级别的时区。有关更多信息，请参见 [配置时区](../../../../administration/management/timezone.md)。

#### `partial_update`

**必需**：否\
**描述**：是否使用部分列更新。有效值：`TRUE` 和 `FALSE`。默认值：`FALSE`，表示禁用此功能。

#### `merge_condition`

**必需**：否\
**描述**：指定要用作条件以确定是否更新数据的列名。只有当要导入此列的数据的值大于或等于此列的当前值时，数据才会被更新。**注意**<br />只有主键表支持条件更新。您指定的列不能是主键列。

#### `format`

**必需**：否\
**描述**：要导入的数据的格式。有效值：`CSV`、`JSON` 和 `Avro`（自 v3.0.1 起支持）。默认值：`CSV`。

#### `trim_space`

**必需**：否\
**描述**：指定在数据文件为 CSV 格式时，是否删除数据文件中列分隔符前后的空格。类型：BOOLEAN。默认值：`false`。<br />对于某些数据库，当您将数据导出为 CSV 格式的数据文件时，会在列分隔符中添加空格。根据空格的位置，这些空格被称为前导空格或尾随空格。通过设置 `trim_space` 参数，您可以启用 StarRocks 在数据导入期间删除这些不必要的空格。<br />请注意，StarRocks 不会删除字段中用一对 `enclose` 指定的字符包裹的空格（包括前导空格和尾随空格）。例如，以下字段值使用管道符号（<code class="language-text">&#124;</code>）作为列分隔符，双引号（`"`）作为 `enclose` 指定的字符：<code class="language-text">&#124; "Love StarRocks" &#124;</code>。如果您将 `trim_space` 设置为 `true`，StarRocks 将处理上述字段值为 <code class="language-text">&#124;"Love StarRocks"&#124;</code>。

#### `enclose`     

**必需**：否\
**描述**：指定在数据文件为 CSV 格式时，根据 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180) 用于包裹字段值的字符。类型：单字节字符。默认值：`NONE`。最常见的字符是单引号（`'`）和双引号（`"`）。<br />所有用 `enclose` 指定的字符包裹的特殊字符（包括行分隔符和列分隔符）都被视为普通符号。StarRocks 可以做得比 RFC4180 更多，因为它允许您指定任何单字节字符作为 `enclose` 指定的字符。<br />如果字段值包含 `enclose` 指定的字符，您可以使用相同的字符来转义该 `enclose` 指定的字符。例如，您将 `enclose` 设置为 `"`，字段值为 `a "quoted" c`。在这种情况下，您可以将字段值输入为 `"a ""quoted"" c"` 到数据文件中。

#### `escape`

**必需**：否\
**描述**：指定用于转义各种特殊字符（如行分隔符、列分隔符、转义字符和 `enclose` 指定的字符）的字符，这些字符随后被 StarRocks 视为普通字符，并被解析为它们所在字段值的一部分。类型：单字节字符。默认值：`NONE`。最常见的字符是斜杠（`\`），在 SQL 语句中必须写为双斜杠（`\\`）。<br />**注意**<br />`escape` 指定的字符适用于每对 `enclose` 指定的字符的内部和外部。<br />以下是两个示例：<br /><ul><li>当您将 `enclose` 设置为 `"` 并将 `escape` 设置为 `\` 时，StarRocks 将 `"say \"Hello world\""` 解析为 `say "Hello world"`。</li><li>假设列分隔符是逗号（`,`）。当您将 `escape` 设置为 `\` 时，StarRocks 将 `a, b\, c` 解析为两个独立的字段值：`a` 和 `b, c`。</li></ul>

#### `strip_outer_array`

**必需**：否\
**描述**：指定是否去除 JSON 格式数据的最外层数组结构。有效值：`true` 和 `false`。默认值：`false`。在实际业务场景中，JSON 格式数据可能具有由一对方括号 `[]` 表示的最外层数组结构。在这种情况下，我们建议您将此参数设置为 `true`，以便 StarRocks 删除最外层的方括号 `[]` 并将每个内部数组作为单独的数据记录加载。如果将此参数设置为 `false`，StarRocks 会将整个 JSON 格式数据解析为一个数组并将该数组作为单个数据记录加载。以 JSON 格式数据 `[{"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` 为例。如果将此参数设置为 `true`，`{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 将被解析为两个独立的数据记录并加载到两个 StarRocks 数据行中。

#### `jsonpaths`  

**必需**：否\
**描述**：要从 JSON 格式数据中加载的字段名称。此参数的值是一个有效的 JsonPath 表达式。有关更多信息，请参见本主题中的 [StarRocks 表包含通过表达式生成值的派生列](#starrocks-table-contains-derived-columns-whose-values-are-generated-by-using-expressions)。

#### `json_root`

**必需**：否\
**描述**：要加载的 JSON 格式数据的根元素。StarRocks 通过 `json_root` 提取根节点的元素进行解析。默认情况下，此参数的值为空，表示将加载所有 JSON 格式数据。有关更多信息，请参见本主题中的 [指定要加载的 JSON 格式数据的根元素](#specify-the-root-element-of-the-json-formatted-data-to-be-loaded)。

#### `task_consume_second`

**必需**：否\
**描述**：指定 Routine Load 作业中每个 Routine Load 任务的最大数据消费时间。单位：秒。与 [FE 动态参数](../../../../administration/management/FE_configuration.md) `routine_load_task_consume_second`（适用于集群中的所有 Routine Load 作业）不同，此参数特定于单个 Routine Load 作业，更加灵活。此参数自 v3.1.0 起支持。<ul> <li>当未配置 `task_consume_second` 和 `task_timeout_second` 时，StarRocks 使用 FE 动态参数 `routine_load_task_consume_second` 和 `routine_load_task_timeout_second` 控制导入行为。</li> <li>仅配置 `task_consume_second` 时，`task_timeout_second` 的默认值计算为 `task_consume_second` * 4。</li> <li>仅配置 `task_timeout_second` 时，`task_consume_second` 的默认值计算为 `task_timeout_second`/4。</li> </ul>

#### `task_timeout_second`

**必需**：否\
**描述**：指定 Routine Load 作业中每个 Routine Load 任务的超时时间。单位：秒。与 [FE 动态参数](../../../../administration/management/FE_configuration.md) `routine_load_task_timeout_second`（适用于集群中的所有 Routine Load 作业）不同，此参数特定于单个 Routine Load 作业，更加灵活。此参数自 v3.1.0 起支持。<ul> <li>当未配置 `task_consume_second` 和 `task_timeout_second` 时，StarRocks 使用 FE 动态参数 `routine_load_task_consume_second` 和 `routine_load_task_timeout_second` 控制导入行为。</li> <li>仅配置 `task_timeout_second` 时，`task_consume_second` 的默认值计算为 `task_timeout_second`/4。</li> <li>仅配置 `task_consume_second` 时，`task_timeout_second` 的默认值计算为 `task_consume_second` * 4。</li> </ul>

#### `pause_on_fatal_parse_error`

**必需**：否\
**描述**：指定在遇到不可恢复的数据解析错误时是否自动暂停作业。有效值：`true` 和 `false`。默认值：`false`。此参数自 v3.3.12/v3.4.2 起支持。<br />此类解析错误通常由非法数据格式引起，例如：<ul><li>导入 JSON 数组但未设置 `strip_outer_array`。</li><li>导入 JSON 数据，但 Kafka 消息包含非法 JSON，如 `abcd`。</li></ul>

#### `data_source`, `data_source_properties`

必需。数据源及相关属性。

```sql
FROM <data_source>
 ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

`data_source`

必需。要导入的数据源。有效值：`KAFKA`。

`data_source_properties`

数据源的属性。

#### `kafka_broker_list`

**必需**：是\
**描述**：Kafka 的 broker 连接信息。格式为 `<kafka_broker_ip>:<broker_ port>`。多个 broker 用逗号（,）分隔。Kafka broker 使用的默认端口是 `9092`。示例：`"kafka_broker_list" = ""xxx.xx.xxx.xx:9092,xxx.xx.xxx.xx:9092"`。

#### `kafka_topic`

**必需**：是\
**描述**：要消费的 Kafka 主题。一个 Routine Load 作业只能从一个主题中消费消息。

#### `kafka_partitions`

**必需**：否\
**描述**：要消费的 Kafka 分区，例如，`"kafka_partitions" = "0, 1, 2, 3"`。如果未指定此属性，默认情况下会消费所有分区。

#### `kafka_offsets`

**必需**：否\
**描述**：指定在 `kafka_partitions` 中从哪个起始偏移量开始消费数据。如果未指定此属性，Routine Load 作业将从 `kafka_partitions` 中的最新偏移量开始消费数据。有效值：<ul><li>特定偏移量：从特定偏移量开始消费数据。</li><li>`OFFSET_BEGINNING`：从最早的偏移量开始消费数据。</li><li>`OFFSET_END`：从最新的偏移量开始消费数据。</li></ul>多个起始偏移量用逗号（,）分隔，例如，`"kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000"`。

#### `property.kafka_default_offsets`

**必需**：否\
**描述**：所有消费者分区的默认起始偏移量。此属性支持的值与 `kafka_offsets` 属性的值相同。

#### `confluent.schema.registry.url`

**必需**：否\
**描述**：注册 Avro schema 的 Schema Registry 的 URL。StarRocks 使用此 URL 检索 Avro schema。格式如下：<br />`confluent.schema.registry.url = http[s]://[<schema-registry-api-key>:<schema-registry-api-secret>@]<hostname or ip address>[:<port>]`

### 更多数据源相关属性

您可以指定其他数据源（Kafka）相关属性，这相当于使用 Kafka 命令行 `--property`。有关更多支持的属性，请参见 [librdkafka 配置属性](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md) 中的 Kafka 消费者客户端属性。

:::note
如果属性的值是文件名，请在文件名前添加关键字 `FILE:`。有关如何创建文件的信息，请参见 [CREATE FILE](../../cluster-management/file/CREATE_FILE.md)。
:::

- **指定要消费的所有分区的默认初始偏移量**

```SQL
"property.kafka_default_offsets" = "OFFSET_BEGINNING"
```

- **指定 Routine Load 作业使用的消费者组 ID**

```SQL
"property.group.id" = "group_id_0"
```

如果未指定 `property.group.id`，StarRocks 会根据 Routine Load 作业的名称生成一个随机值，格式为 `{job_name}_{random uuid}`，例如 `simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`。

- **指定 BE 访问 Kafka 使用的安全协议及相关参数**

  安全协议可以指定为 `plaintext`（默认）、`ssl`、`sasl_plaintext` 或 `sasl_ssl`。您需要根据指定的安全协议配置相关参数。

  当安全协议设置为 `sasl_plaintext` 或 `sasl_ssl` 时，支持以下 SASL 认证机制：

  - PLAIN
  - SCRAM-SHA-256 和 SCRAM-SHA-512
  - OAUTHBEARER
  - GSSAPI (Kerberos)

  **示例：**

  - 使用 SSL 安全协议访问 Kafka：

    ```SQL
    "property.security.protocol" = "ssl", -- 指定安全协议为 SSL。
    "property.ssl.ca.location" = "FILE:ca-cert", -- 用于验证 Kafka broker 密钥的 CA 证书的文件或目录路径。
    -- 如果 Kafka 服务器启用了客户端认证，还需要以下三个参数：
    "property.ssl.certificate.location" = "FILE:client.pem", -- 用于认证的客户端公钥路径。
    "property.ssl.key.location" = "FILE:client.key", -- 用于认证的客户端私钥路径。
    "property.ssl.key.password" = "xxxxxx" -- 客户端私钥的密码。
    ```

  - 使用 SASL_PLAINTEXT 安全协议和 SASL/PLAIN 认证机制访问 Kafka：

    ```SQL
    "property.security.protocol" = "SASL_PLAINTEXT", -- 指定安全协议为 SASL_PLAINTEXT。
    "property.sasl.mechanism" = "PLAIN", -- 指定 SASL 机制为 PLAIN，这是一个简单的用户名/密码认证机制。
    "property.sasl.username" = "admin",  -- SASL 用户名。
    "property.sasl.password" = "xxxxxx"  -- SASL 密码。
    ```

  - 使用 SASL_PLAINTEXT 安全协议和 SASL/GSSAPI (Kerberos) 认证机制访问 Kafka：

    ```sql
    "property.security.protocol" = "SASL_PLAINTEXT", -- 指定安全协议为 SASL_PLAINTEXT。
    "property.sasl.mechanism" = "GSSAPI", -- 指定 SASL 认证机制为 GSSAPI。默认值为 GSSAPI。
    "property.sasl.kerberos.service.name" = "kafka", -- broker 服务名称。默认值为 kafka。
    "property.sasl.kerberos.keytab" = "/home/starrocks/starrocks.keytab", -- 客户端 keytab 位置。
    "property.sasl.kerberos.principal" = "starrocks@YOUR.COM" -- Kerberos 主体。
    ```

    :::note

    - 自 StarRocks v3.1.4 起，支持 SASL/GSSAPI (Kerberos) 认证。
    - 需要在 BE 机器上安装 SASL 相关模块。

        ```bash
        # Debian/Ubuntu:
        sudo apt-get install libsasl2-modules-gssapi-mit libsasl2-dev
        # CentOS/Redhat:
        sudo yum install cyrus-sasl-gssapi cyrus-sasl-devel
        ```

    :::

### FE 和 BE 配置项

有关与 Routine Load 相关的 FE 和 BE 配置项，请参见 [配置项](../../../../administration/management/FE_configuration.md)。

## 列映射

### 配置 CSV 格式数据的列映射

如果 CSV 格式数据的列可以按顺序一对一映射到 StarRocks 表的列，则无需配置数据与 StarRocks 表之间的列映射。

如果 CSV 格式数据的列不能按顺序一对一映射到 StarRocks 表的列，则需要使用 `columns` 参数配置数据文件与 StarRocks 表之间的列映射。这包括以下两种用例：

- **列数相同但列顺序不同。此外，数据文件中的数据不需要在加载到匹配的 StarRocks 表列之前通过函数进行计算。**

  - 在 `columns` 参数中，您需要按照数据文件列的排列顺序指定 StarRocks 表列的名称。

  - 例如，StarRocks 表由三列组成，按顺序为 `col1`、`col2` 和 `col3`，数据文件也由三列组成，可以按顺序映射到 StarRocks 表列 `col3`、`col2` 和 `col1`。在这种情况下，您需要指定 `"columns: col3, col2, col1"`。

- **列数不同且列顺序不同。此外，数据文件中的数据需要在加载到匹配的 StarRocks 表列之前通过函数进行计算。**

  在 `columns` 参数中，您需要按照数据文件列的排列顺序指定 StarRocks 表列的名称，并指定要用于计算数据的函数。以下是两个示例：

  - StarRocks 表由三列组成，按顺序为 `col1`、`col2` 和 `col3`。数据文件由四列组成，其中前三列可以按顺序映射到 StarRocks 表列 `col1`、`col2` 和 `col3`，第四列不能映射到任何 StarRocks 表列。在这种情况下，您需要为数据文件的第四列临时指定一个名称，并且临时名称必须与任何 StarRocks 表列名称不同。例如，您可以指定 `"columns: col1, col2, col3, temp"`，其中数据文件的第四列临时命名为 `temp`。
  - StarRocks 表由三列组成，按顺序为 `year`、`month` 和 `day`。数据文件仅由一列组成，该列包含 `yyyy-mm-dd hh:mm:ss` 格式的日期和时间值。在这种情况下，您可以指定 `"columns: col, year = year(col), month=month(col), day=day(col)"`，其中 `col` 是数据文件列的临时名称，函数 `year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于从数据文件列 `col` 中提取数据并将数据加载到映射的 StarRocks 表列中。例如，`year = year(col)` 用于从数据文件列 `col` 中提取 `yyyy` 数据并将数据加载到 StarRocks 表列 `year` 中。

有关更多示例，请参见 [配置列映射](#configure-column-mapping)。

### 配置 JSON 格式或 Avro 格式数据的列映射

:::note
自 v3.0.1 起，StarRocks 支持使用 Routine Load 加载 Avro 数据。当您加载 JSON 或 Avro 数据时，列映射和转换的配置相同。因此，在本节中，使用 JSON 数据作为示例介绍配置。
:::

如果 JSON 格式数据的键与 StarRocks 表的列同名，您可以使用简单模式加载 JSON 格式数据。在简单模式中，您无需指定 `jsonpaths` 参数。此模式要求 JSON 格式数据必须是由大括号 `{}` 表示的对象，例如 `{"category": 1, "author": 2, "price": "3"}`。在此示例中，`category`、`author` 和 `price` 是键名，这些键可以按名称一对一映射到 StarRocks 表的列 `category`、`author` 和 `price`。有关示例，请参见 [简单模式](#starrocks-table-column-names-consistent-with-json-key-names)。

如果 JSON 格式数据的键与 StarRocks 表的列不同名，您可以使用匹配模式加载 JSON 格式数据。在匹配模式中，您需要使用 `jsonpaths` 和 `COLUMNS` 参数指定 JSON 格式数据与 StarRocks 表之间的列映射：

- 在 `jsonpaths` 参数中，按照 JSON 格式数据中键的排列顺序指定 JSON 键。
- 在 `COLUMNS` 参数中，指定 JSON 键与 StarRocks 表列之间的映射：
  - `COLUMNS` 参数中指定的列名按顺序一对一映射到 JSON 格式数据。
  - `COLUMNS` 参数中指定的列名按名称一对一映射到 StarRocks 表列。

有关示例，请参见 [StarRocks 表包含通过表达式生成值的派生列](#starrocks-table-contains-derived-columns-whose-values-are-generated-by-using-expressions)。

## 示例

<WarehouseExample />

### 导入 CSV 格式数据

本节以 CSV 格式数据为例，介绍如何通过各种参数设置和组合来满足您的多样化导入需求。

**准备数据集**

假设您要从名为 `ordertest1` 的 Kafka 主题中导入 CSV 格式数据。数据集中的每条消息包括六列：订单 ID、支付日期、客户姓名、国籍、性别和价格。

```plaintext
2020050802,2020-05-08,Johann Georg Faust,Deutschland,male,895
2020050802,2020-05-08,Julien Sorel,France,male,893
2020050803,2020-05-08,Dorian Grey,UK,male,1262
2020050901,2020-05-09,Anna Karenina,Russia,female,175
2020051001,2020-05-10,Tess Durbeyfield,US,female,986
2020051101,2020-05-11,Edogawa Conan,japan,male,8924
```

**创建表**

根据 CSV 格式数据的列，在数据库 `example_db` 中创建一个名为 `example_tbl1` 的表。

```SQL
CREATE TABLE example_db.example_tbl1 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `gender` varchar(26) NULL COMMENT "Gender", 
    `price` double NULL COMMENT "Price") 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`); 
```

#### 从指定分区的指定偏移量开始消费数据

如果 Routine Load 作业需要从指定的分区和偏移量开始消费数据，您需要配置参数 `kafka_partitions` 和 `kafka_offsets`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    "kafka_partitions" ="0,1,2,3,4", -- 要消费的分区
    "kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000" -- 对应的初始偏移量
);
```

#### 通过增加任务并行度提高导入性能

为了提高导入性能并避免累积消费，您可以在创建 Routine Load 作业时通过增加 `desired_concurrent_number` 值来增加任务并行度。任务并行度允许将一个 Routine Load 作业拆分为尽可能多的并行任务。

请注意，实际任务并行度由以下多个参数的最小值决定：

```SQL
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

:::note
最大实际任务并行度是存活的 BE 节点数或要消费的分区数。
:::

因此，当存活的 BE 节点数和要消费的分区数大于其他两个参数 `max_routine_load_task_concurrent_num` 和 `desired_concurrent_number` 的值时，您可以增加其他两个参数的值以增加实际任务并行度。

假设要消费的分区数为 7，存活的 BE 节点数为 5，`max_routine_load_task_concurrent_num` 的默认值为 `5`。如果您想增加实际任务并行度，可以将 `desired_concurrent_number` 设置为 `5`（默认值为 `3`）。在这种情况下，实际任务并行度 `min(5,7,5,5)` 被配置为 `5`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"desired_concurrent_number" = "5" -- 将 desired_concurrent_number 的值设置为 5
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 配置列映射

如果 CSV 格式数据中的列顺序与目标表中的列不一致，假设 CSV 格式数据中的第五列不需要导入到目标表中，您需要通过 `COLUMNS` 参数指定 CSV 格式数据与目标表之间的列映射。

**目标数据库和表**

根据 CSV 格式数据中的列，在目标数据库 `example_db` 中创建目标表 `example_tbl2`。在此场景中，您需要创建五个列，分别对应 CSV 格式数据中的五个列，除了存储性别的第五列。

```SQL
CREATE TABLE example_db.example_tbl2 ( 
    `order_id` bigint NOT NULL COMMENT "Order ID",
    `pay_dt` date NOT NULL COMMENT "Payment date", 
    `customer_name` varchar(26) NULL COMMENT "Customer name", 
    `nationality` varchar(26) NULL COMMENT "Nationality", 
    `price` double NULL COMMENT "Price"
) 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(order_id); 
```

**Routine Load 作业**

在此示例中，由于 CSV 格式数据中的第五列不需要加载到目标表中，因此在 `COLUMNS` 中将第五列临时命名为 `temp_gender`，其他列直接映射到表 `example_tbl2`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, temp_gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 设置过滤条件

如果您只想加载满足某些条件的数据，可以在 `WHERE` 子句中设置过滤条件，例如，`price > 100.`

```SQL
CREATE ROUTINE LOAD example_db.example_tbl2_ordertest1 ON example_tbl2
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price),
WHERE price > 100 -- 设置过滤条件
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 启用严格模式以过滤掉包含 NULL 值的行

在 `PROPERTIES` 中，您可以设置 `"strict_mode" = "true"`，这意味着 Routine Load 作业处于严格模式。如果源列中存在 `NULL` 值，但目标 StarRocks 表列不允许 NULL 值，则包含源列中 NULL 值的行将被过滤掉。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"strict_mode" = "true" -- 启用严格模式
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 设置错误容忍度

如果您的业务场景对不合格数据的容忍度较低，您需要通过配置参数 `max_batch_rows` 和 `max_error_number` 来设置错误检测窗口和最大错误数据行数。当错误检测窗口内的错误数据行数超过 `max_error_number` 的值时，Routine Load 作业将暂停。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"max_batch_rows" = "100000",-- max_batch_rows 的值乘以 10 等于错误检测窗口。
"max_error_number" = "100" -- 错误检测窗口内允许的最大错误数据行数。
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 指定安全协议为 SSL 并配置相关参数

如果您需要指定 BE 访问 Kafka 使用的安全协议为 SSL，您需要配置 `"property.security.protocol" = "ssl"` 和相关参数。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
    "format" = "json"
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    -- 指定安全协议为 SSL。
    "property.security.protocol" = "ssl",
    -- CA 证书的位置。
    "property.ssl.ca.location" = "FILE:ca-cert",
    -- 如果启用了 Kafka 客户端认证，您需要配置以下属性：
    -- Kafka 客户端公钥的位置。
    "property.ssl.certificate.location" = "FILE:client.pem",
    -- Kafka 客户端私钥的位置。
    "property.ssl.key.location" = "FILE:client.key",
    -- Kafka 客户端私钥的密码。
    "property.ssl.key.password" = "abcdefg"
);
```

#### 设置 trim_space、enclose 和 escape

假设您要从名为 `test_csv` 的 Kafka 主题中导入 CSV 格式数据。数据集中的每条消息包括六列：订单 ID、支付日期、客户姓名、国籍、性别和价格。

```Plaintext
 "2020050802" , "2020-05-08" , "Johann Georg Faust" , "Deutschland" , "male" , "895"
 "2020050802" , "2020-05-08" , "Julien Sorel" , "France" , "male" , "893"
 "2020050803" , "2020-05-08" , "Dorian Grey\,Lord Henry" , "UK" , "male" , "1262"
 "2020050901" , "2020-05-09" , "Anna Karenina" , "Russia" , "female" , "175"
 "2020051001" , "2020-05-10" , "Tess Durbeyfield" , "US" , "female" , "986"
 "2020051101" , "2020-05-11" , "Edogawa Conan" , "japan" , "male" , "8924"
```

如果您想将 Kafka 主题 `test_csv` 中的所有数据导入 `example_tbl1`，并希望去除列分隔符前后的空格，并将 `enclose` 设置为 `"`，`escape` 设置为 `\`，请运行以下命令：

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_test_csv ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
    "trim_space"="true",
    "enclose"="\"",
    "escape"="\\",
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic"="test_csv",
    "property.kafka_default_offsets"="OFFSET_BEGINNING"
);
```

### 导入 JSON 格式数据

#### StarRocks 表列名与 JSON 键名一致

**准备数据集**

例如，Kafka 主题 `ordertest2` 中存在以下 JSON 格式数据。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

:::note
每个 JSON 对象必须在一个 Kafka 消息中。否则，会出现解析 JSON 格式数据失败的错误。
:::

**目标数据库和表**

在 StarRocks 集群的目标数据库 `example_db` 中创建表 `example_tbl3`。列名与 JSON 格式数据中的键名一致。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL COMMENT "Price") 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time)
DISTRIBUTED BY HASH(commodity_id); 
```

**Routine Load 作业**

您可以为 Routine Load 作业使用简单模式。也就是说，创建 Routine Load 作业时无需指定 `jsonpaths` 和 `COLUMNS` 参数。StarRocks 根据目标表 `example_tbl3` 的列名提取 Kafka 集群中 `ordertest2` 主题的 JSON 格式数据的键，并将 JSON 格式数据加载到目标表中。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest2 ON example_tbl3
PROPERTIES
(
    "format" = "json"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

:::note
- 如果 JSON 格式数据的最外层是数组结构，您需要在 `PROPERTIES` 中设置 `"strip_outer_array"="true"` 以去除最外层的数组结构。此外，当您需要指定 `jsonpaths` 时，整个 JSON 格式数据的根元素是展平的 JSON 对象，因为 JSON 格式数据的最外层数组结构已被去除。
- 您可以使用 `json_root` 指定 JSON 格式数据的根元素。
:::

#### StarRocks 表包含通过表达式生成值的派生列

**准备数据集**

例如，Kafka 集群的 `ordertest2` 主题中存在以下 JSON 格式数据。

```SQL
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

**目标数据库和表**

在 StarRocks 集群的数据库 `example_db` 中创建一个名为 `example_tbl4` 的表。列 `pay_dt` 是一个派生列，其值通过计算 JSON 格式数据中键 `pay_time` 的值生成。

```SQL
CREATE TABLE example_db.example_tbl4 ( 
    `commodity_id` varchar(26) NULL, 
    `customer_name` varchar(26) NULL, 
    `country` varchar(26) NULL,
    `pay_time` bigint(20) NULL,  
    `pay_dt` date NULL, 
    `price` double SUM NULL) 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`,`pay_dt`) 
DISTRIBUTED BY HASH(`commodity_id`); 
```

**Routine Load 作业**

您可以为 Routine Load 作业使用匹配模式。也就是说，创建 Routine Load 作业时需要指定 `jsonpaths` 和 `COLUMNS` 参数。

您需要在 `jsonpaths` 参数中指定 JSON 格式数据的键，并按顺序排列。

由于 JSON 格式数据中键 `pay_time` 的值需要在存储到 `example_tbl4` 表的 `pay_dt` 列之前转换为 DATE 类型，因此您需要在 `COLUMNS` 中通过 `pay_dt=from_unixtime(pay_time,'%Y%m%d')` 指定计算。JSON 格式数据中的其他键的值可以直接映射到 `example_tbl4` 表。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl4_ordertest2 ON example_tbl4
COLUMNS(commodity_id, customer_name, country, pay_time, pay_dt=from_unixtime(pay_time, '%Y%m%d'), price)
PROPERTIES
(
    "format" = "json",
    "jsonpaths" = "[\"$.commodity_id\",\"$.customer_name\",\"$.country\",\"$.pay_time\",\"$.price\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

:::note
- 如果 JSON 数据的最外层是数组结构，您需要在 `PROPERTIES` 中设置 `"strip_outer_array"="true"` 以去除最外层的数组结构。此外，当您需要指定 `jsonpaths` 时，整个 JSON 数据的根元素是展平的 JSON 对象，因为 JSON 数据的最外层数组结构已被去除。
- 您可以使用 `json_root` 指定 JSON 格式数据的根元素。
:::

#### StarRocks 表包含通过 CASE 表达式生成值的派生列

**准备数据集**

例如，Kafka 主题 `topic-expr-test` 中存在以下 JSON 格式数据。

```JSON
{"key1":1, "key2": 21}
{"key1":12, "key2": 22}
{"key1":13, "key2": 23}
{"key1":14, "key2": 24}
```

**目标数据库和表**

在 StarRocks 集群的数据库 `example_db` 中创建一个名为 `tbl_expr_test` 的表。目标表 `tbl_expr_test` 包含两列，其中 `col2` 列的值需要通过对 JSON 数据使用 CASE 表达式进行计算。

```SQL
CREATE TABLE tbl_expr_test (
    col1 string, col2 string)
DISTRIBUTED BY HASH (col1);
```

**Routine Load 作业**

由于目标表中 `col2` 列的值是通过 CASE 表达式生成的，因此您需要在 Routine load 作业的 `COLUMNS` 参数中指定相应的表达式。

```SQL
CREATE ROUTINE LOAD rl_expr_test ON tbl_expr_test
COLUMNS (
      key1,
      key2,
      col1 = key1,
      col2 = CASE WHEN key1 = "1" THEN "key1=1" 
                  WHEN key1 = "12" THEN "key1=12"
                  ELSE "nothing" END) 
PROPERTIES ("format" = "json")
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "topic-expr-test"
);
```

**查询 StarRocks 表**

查询 StarRocks 表。结果显示 `col2` 列中的值是 CASE 表达式的输出。

```SQL
MySQL [example_db]> SELECT * FROM tbl_expr_test;
+------+---------+
| col1 | col2    |
+------+---------+
| 1    | key1=1  |
| 12   | key1=12 |
| 13   | nothing |
| 14   | nothing |
+------+---------+
4 rows in set (0.015 sec)
```

#### 指定要加载的 JSON 格式数据的根元素

您需要使用 `json_root` 指定要加载的 JSON 格式数据的根元素，值必须是一个有效的 JsonPath 表达式。

**准备数据集**

例如，Kafka 集群的 `ordertest3` 主题中存在以下 JSON 格式数据。要加载的 JSON 格式数据的根元素是 `$.RECORDS`。

```SQL
{"RECORDS":[{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875},{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895},{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}]}
```

**目标数据库和表**

在 StarRocks 集群的数据库 `example_db` 中创建一个名为 `example_tbl3` 的表。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    commodity_id varchar(26) NULL, 
    customer_name varchar(26) NULL, 
    country varchar(26) NULL, 
    pay_time bigint(20) NULL, 
    price double SUM NULL) 
AGGREGATE KEY(commodity_id,customer_name,country,pay_time) 
ENGINE=OLAP
DISTRIBUTED BY HASH(commodity_id); 
```

**Routine Load 作业**

您可以在 `PROPERTIES` 中设置 `"json_root" = "$.RECORDS"` 以指定要加载的 JSON 格式数据的根元素。此外，由于要加载的 JSON 格式数据是数组结构，您还必须设置 `"strip_outer_array" = "true"` 以去除最外层的数组结构。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest3 ON example_tbl3
PROPERTIES
(
    "format" = "json",
    "json_root" = "$.RECORDS",
    "strip_outer_array" = "true"
 )
FROM KAFKA
(
    "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

### 导入 Avro 格式数据

自 v3.0.1 起，StarRocks 支持使用 Routine Load 加载 Avro 数据。

#### Avro schema 简单

假设 Avro schema 相对简单，并且您需要加载 Avro 数据的所有字段。

**准备数据集**

- **Avro schema**

    1. 创建以下 Avro schema 文件 `avro_schema1.avsc`：

        ```json
        {
            "type": "record",
            "name": "sensor_log",
            "fields" : [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "checked", "type" : "boolean"},
                {"name": "data", "type": "double"},
                {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}}  
            ]
        }
        ```

    2. 在 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) 中注册 Avro schema。

- **Avro 数据**

准备 Avro 数据并将其发送到 Kafka 主题 `topic_1`。

**目标数据库和表**

根据 Avro 数据的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log1`。表的列名必须与 Avro 数据中的字段名匹配。有关 Avro 数据加载到 StarRocks 时的数据类型映射，请参见 [数据类型映射](#Data types mapping)。

```SQL
CREATE TABLE sensor.sensor_log1 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `data` double NULL COMMENT "sensor data", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type"
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load 作业**

您可以为 Routine Load 作业使用简单模式。也就是说，创建 Routine Load 作业时无需指定 `jsonpaths` 参数。执行以下语句提交一个名为 `sensor_log_load_job1` 的 Routine Load 作业，以消费 Kafka 主题 `topic_1` 中的 Avro 消息并将数据加载到数据库 `sensor` 中的表 `sensor_log1`。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job1 ON sensor_log1  
PROPERTIES  
(  
  "format" = "avro"  
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic"= "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro schema 包含嵌套的 record 类型字段

假设 Avro schema 包含嵌套的 record 类型字段，并且您需要将嵌套 record 类型字段中的子字段加载到 StarRocks。

**准备数据集**

- **Avro schema**

    1. 创建以下 Avro schema 文件 `avro_schema2.avsc`。外部 Avro 记录包括五个字段，按顺序为 `id`、`name`、`checked`、`sensor_type` 和 `data`。字段 `data` 包含一个嵌套的 record `data_record`。

        ```JSON
        {
            "type": "record",
            "name": "sensor_log",
            "fields" : [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "checked", "type" : "boolean"},
                {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
                {"name": "data", "type": 
                    {
                        "type": "record",
                        "name": "data_record",
                        "fields" : [
                            {"name": "data_x", "type" : "boolean"},
                            {"name": "data_y", "type": "long"}
                        ]
                    }
                }
            ]
        }
        ```

    2. 在 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) 中注册 Avro schema。

- **Avro 数据**

准备 Avro 数据并将其发送到 Kafka 主题 `topic_2`。

**目标数据库和表**

根据 Avro 数据的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log2`。

假设除了加载外部 Record 的字段 `id`、`name`、`checked` 和 `sensor_type` 外，您还需要加载嵌套 Record `data_record` 中的子字段 `data_y`。

```sql
CREATE TABLE sensor.sensor_log2 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load 作业**

提交导入作业，使用 `jsonpaths` 指定需要加载的 Avro 数据字段。请注意，对于嵌套 Record 中的子字段 `data_y`，您需要将其 `jsonpath` 指定为 `"$.data.data_y"`。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job2 ON sensor_log2  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro schema 包含 Union 字段

**准备数据集**

假设 Avro schema 包含 Union 字段，并且您需要将 Union 字段加载到 StarRocks。

- **Avro schema**

    1. 创建以下 Avro schema 文件 `avro_schema3.avsc`。外部 Avro 记录包括五个字段，按顺序为 `id`、`name`、`checked`、`sensor_type` 和 `data`。字段 `data` 是 Union 类型，包含两个元素，`null` 和一个嵌套的 record `data_record`。

        ```JSON
        {
            "type": "record",
            "name": "sensor_log",
            "fields" : [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "checked", "type" : "boolean"},
                {"name": "sensor_type", "type": {"type": "enum", "name": "sensor_type_enum", "symbols" : ["TEMPERATURE", "HUMIDITY", "AIR-PRESSURE"]}},
                {"name": "data", "type": [null,
                        {
                            "type": "record",
                            "name": "data_record",
                            "fields" : [
                                {"name": "data_x", "type" : "boolean"},
                                {"name": "data_y", "type": "long"}
                            ]
                        }
                    ]
                }
            ]
        }
        ```

    2. 在 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html) 中注册 Avro schema。

- **Avro 数据**

准备 Avro 数据并将其发送到 Kafka 主题 `topic_3`。

**目标数据库和表**

根据 Avro 数据的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log3`。

假设除了加载外部 Record 的字段 `id`、`name`、`checked` 和 `sensor_type` 外，您还需要加载 Union 类型字段 `data` 中元素 `data_record` 的字段 `data_y`。

```sql
CREATE TABLE sensor.sensor_log3 ( 
    `id` bigint NOT NULL COMMENT "sensor id",
    `name` varchar(26) NOT NULL COMMENT "sensor name", 
    `checked` boolean NOT NULL COMMENT "checked", 
    `sensor_type` varchar(26) NOT NULL COMMENT "sensor type",
    `data_y` long NULL COMMENT "sensor data" 
) 
ENGINE=OLAP 
DUPLICATE KEY (id) 
DISTRIBUTED BY HASH(`id`); 
```

**Routine Load 作业**

提交导入作业，使用 `jsonpaths` 指定需要加载的 Avro 数据字段。请注意，对于字段 `data_y`，您需要将其 `jsonpath` 指定为 `"$.data.data_y"`。

```sql
CREATE ROUTINE LOAD sensor.sensor_log_load_job3 ON sensor_log3  
PROPERTIES  
(  
  "format" = "avro",
  "jsonpaths" = "[\"$.id\",\"$.name\",\"$.checked\",\"$.sensor_type\",\"$.data.data_y\"]"
)  
FROM KAFKA  
(  
  "kafka_broker_list" = "<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>,...",
  "confluent.schema.registry.url" = "http://172.xx.xxx.xxx:8081",  
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

当 Union 类型字段 `data` 的值为 `null` 时，加载到 StarRocks 表中 `data_y` 列的值为 `null`。当 Union 类型字段 `data` 的值为数据记录时，加载到 `data_y` 列的值为 Long 类型。
