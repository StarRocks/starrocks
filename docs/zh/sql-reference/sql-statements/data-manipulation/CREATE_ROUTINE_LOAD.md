---
displayed_sidebar: "Chinese"
---

# CREATE ROUTINE LOAD

## 功能

Routine Load 支持持续消费 Apache Kafka® 的消息并导入至 StarRocks 中。Routine Load 支持 Kafka 中消息的格式为 CSV、JSON、Avro (自 v3.0.1)，并且访问 Kafka 时，支持多种安全协议，包括 `plaintext`、`ssl`、`sasl_plaintext` 和 `sasl_ssl`。

本文介绍 CREATE ROUTINE LOAD 的语法、参数说明和示例。

> **说明**
>
> - Routine Load 的应用场景、基本原理和基本操作，请参见 [使用 Routine Load 导入数据](../../../loading/RoutineLoad.md)。
> - Routine Load 操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../account-management/GRANT.md) 给用户赋权。

## 语法

```SQL
CREATE ROUTINE LOAD <database_name>.<job_name> ON <table_name>
[load_properties]
[job_properties]
FROM data_source
[data_source_properties]
```

## 参数说明

### `database_name`、`job_name`、`table_name`

`database_name`

选填，目标数据库的名称。

`job_name`

必填，导入作业的名称。一张表可能有多个导入作业，建议您利用具有辨识度的信息（例如 Kafka Topic 名称、创建导入作业的大致时间等）来设置具有意义的导入作业名称，用于区分多个导入作业。同一数据库内，导入作业的名称必须唯一。

`table_name`

必填，目标表的名称。

### `load_properties`

选填。源数据的属性。语法：

```SQL
[COLUMNS TERMINATED BY '<column_separator>'],
[ROWS TERMINATED BY '<row_separator>'],
[COLUMNS (<column1_name>[,<column2_name>,<column_assignment>,... ])],
[WHERE <expr>],
[PARTITION (<partition1_name>[,<partition2_name>,...])]
[TEMPORARY PARTITION (<temporary_partition1_name>[,<temporary_partition2_name>,...])]
```

如果导入 CSV 格式的数据，则可以指定列分隔符，默认为`\t`，即 Tab。例如可以输入 `COLUMNS TERMINATED BY ","`。指定列分隔符为逗号(,)。

> **说明**
>
> - 必须确保这里指定的列分隔符与源数据中的列分隔符一致。
> - StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。
> - 空值 (null) 用 `\N` 表示。比如，源数据一共有三列，其中某行数据的第一列、第三列数据分别为 `a` 和 `b`，第二列没有数据，则第二列需要用 `\N` 来表示空值，写作 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示第二列是一个空字符串。

`ROWS TERMINATED BY`

用于指定源数据中的行分隔符。如果不指定该参数，则默认为 `\n`。

`COLUMNS`

源数据和目标表之间的列映射和转换关系。详细说明，请参见[列映射和转换关系](#列映射和转换关系)。

- `column_name`：映射列，源数据中这类列的值可以直接落入目标表的列中，不需要进行计算。
- `column_assignment`：衍生列，格式为 `column_name = expr`，源数据中这类列的值需要基于表达式 `expr` 进行计算后，才能落入目标表的列中。 建议将衍生列排在映射列之后，因为 StarRocks 先解析映射列，再解析衍生列。

> **说明**
>
> 以下情况不需要设置 `COLUMNS` 参数：
>
> - 待导入 CSV 数据中的列与目标表中列的数量和顺序一致。
> - 待导入 JSON 数据中的 Key 名与目标表中的列名一致。

`WHERE`

设置过滤条件，只有满足过滤条件的数据才会导入到 StarRocks 中。例如只希望导入 `col1` 大于 `100` 并且 `col2` 等于 `1000` 的数据行，则可以输入 `WHERE col1 > 100 and col2 = 1000`。

> **说明**
>
> 过滤条件中指定的列可以是源数据中本来就存在的列，也可以是基于源数据的列生成的衍生列。

`PARTITION`

将数据导入至目标表的指定分区中。如果不指定分区，则会将数据自动导入至其对应的分区中。 示例：

```SQL
PARTITION(p1, p2, p3)
```

### `job_properties`

必填。导入作业的属性。语法：

```SQL
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

参数说明如下：

| **参数**                  | **是否必选** | **说明**                                                     |
| ------------------------- | ------------ | ------------------------------------------------------------ |
| desired_concurrent_number | 否           | 单个 Routine Load 导入作业的**期望**任务并发度，表示期望一个导入作业最多被分成多少个任务并行执行。默认值为 `3`。 但是**实际**任务并行度由如下多个参数组成的公式决定，并且实际任务并行度的上限为 BE 节点的数量或者消费分区的数量。`min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)`。<ul> <li>`alive_be_number`：存活的 BE 节点数量。</li><li>`partition_number`：消费分区数量。</li><li>`desired_concurrent_number`：单个Routine Load 导入作业的期望任务并发度。默认值为 `3`。</li><li>`max_routine_load_task_concurrent_num`：Routine Load 导入作业的默认最大任务并行度，默认值为 `5`。该参数为 [FE 动态参数](../../../administration/Configuration.md)。</li></ul> |
| max_batch_interval        | 否           | 任务的调度间隔，即任务多久执行一次。单位：秒。取值范围：`5`～`60`。默认值：`10`。建议取值为导入间隔 10s 以上，否则会因为导入频率过高可能会报错版本数过多。 |
| max_batch_rows            | 否           | 该参数只用于定义错误检测窗口范围，错误检测窗口范围为单个 Routine Load 导入任务所消费的 `10 * max-batch-rows` 行数据，默认为 `10 * 200000 = 2000000`。导入任务时会检测窗口中数据是否存在错误。错误数据是指 StarRocks 无法解析的数据，比如非法的 JSON。 |
| max_error_number          | 否           | 错误检测窗口范围内允许的错误数据行数的上限。当错误数据行数超过该值时，导入作业会暂停，此时您需要执行 [SHOW ROUTINE LOAD](../data-manipulation/SHOW_ROUTINE_LOAD.md)，根据 `ErrorLogUrls`，检查 Kafka 中的消息并且更正错误。默认为 `0`，表示不允许有错误行。错误行不包括通过 WHERE 子句过滤掉的数据。 |
| strict_mode               | 否           | 是否开启严格模式。取值范围：`TRUE` 或者 `FALSE`。默认值：`FALSE`。开启后，如果源数据某列的值为 `NULL`，但是目标表中该列不允许为 `NULL`，则该行数据会被过滤掉。<br />关于该模式的介绍，参见[严格模式](../../../loading/load_concept/strict_mode.md)。|
| log_rejected_record_num | 否 | 指定最多允许记录多少条因数据质量不合格而过滤掉的数据行数。该参数自 3.1 版本起支持。取值范围：`0`、`-1`、大于 0 的正整数。默认值：`0`。<ul><li>取值为 `0` 表示不记录过滤掉的数据行。</li><li>取值为 `-1` 表示记录所有过滤掉的数据行。</li><li>取值为大于 0 的正整数（比如 `n`）表示每个 BE 节点上最多可以记录 `n` 条过滤掉的数据行。</li></ul> |
| timezone                  | 否           | 该参数的取值会影响所有导入涉及的、跟时区设置有关的函数所返回的结果。受时区影响的函数有 strftime、alignment_timestamp 和 from_unixtime 等，具体请参见[设置时区](../../../administration/timezone.md)。导入参数 `timezone` 设置的时区对应[设置时区](../../../administration/timezone.md)中所述的会话级时区。 |
| merge_condition           | 否           | 用于指定作为更新生效条件的列名。这样只有当导入的数据中该列的值大于等于当前值的时候，更新才会生效。参见[通过导入实现数据变更](../../../loading/Load_to_Primary_Key_tables.md)。指定的列必须为非主键列，且仅主键表支持条件更新。 |
| format                    | 否           | 源数据的格式，取值范围：`CSV`、`JSON` 或者 `Avro`（自 v3.0.1）。默认值：`CSV`。|
| trim_space                | 否           | 用于指定是否去除 CSV 文件中列分隔符前后的空格。取值类型：BOOLEAN。默认值：`false`。<br />有些数据库在导出数据为 CSV 文件时，会在列分隔符的前后添加一些空格。根据位置的不同，这些空格可以称为“前导空格”或者“尾随空格”。通过设置该参数，可以使 StarRocks 在导入数据时删除这些不必要的空格。<br />需要注意的是，StarRocks 不会去除被 `enclose` 指定字符括起来的字段内的空格（包括字段的前导空格和尾随空格）。例如，列分隔符是竖线 (<code class="language-text">&#124;</code>)，`enclose` 指定的字符是双引号 (`"`)：<code class="language-text">&#124; "Love StarRocks" &#124;</code>。如果设置 trim_space 为 true，则 StarRocks 处理后的结果数据为 <code class="language-text">&#124;"Love StarRocks"&#124;</code>。|
| enclose                   | 否           | 根据 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180)，用于指定把 CSV 文件中的字段括起来的字符。取值类型：单字节字符。默认值：`NONE`。最常用 `enclose` 字符为单引号 (`'`) 或双引号 (`"`)。<br />被 `enclose` 指定字符括起来的字段内的所有特殊字符（包括行分隔符、列分隔符等）均看做是普通符号。比 RFC4180 标准更进一步的是，StarRocks 提供的 `enclose` 属性支持设置任意单个字节的字符。<br />如果一个字段内包含了 `enclose` 指定字符，则可以使用同样的字符对 `enclose` 指定字符进行转义。例如，在设置了`enclose` 为双引号 (`"`) 时，字段值 `a "quoted" c` 在 CSV 文件中应该写作 `"a ""quoted"" c"`。 |
| escape                    | 否           | 指定用于转义的字符。用来转义各种特殊字符，比如行分隔符、列分隔符、转义符、`enclose` 指定字符等，使 StarRocks 把这些特殊字符当做普通字符而解析成字段值的一部分。取值类型：单字节字符。默认值：`NONE`。最常用的 `escape` 字符为斜杠 (`\`)，在 SQL 语句中应该写作双斜杠 (`\\`)。<br />`escape` 指定字符同时作用于 `enclose` 指定字符的内部和外部。<br />以下为两个示例：<br /><ul><li>当设置 `enclose` 为双引号 (`"`) 、`escape` 为斜杠 (`\`) 时，StarRocks 会把 `"say \"Hello world\""` 解析成一个字段值 `say "Hello world"`。</li><li>假设列分隔符为逗号 (`,`) ，当设置 `escape` 为斜杠 (`\`) ，StarRocks 会把 `a, b\, c` 解析成 `a` 和 `b, c` 两个字段值。</li></ul> |
| strip_outer_array         | 否           | 是否裁剪 JSON 数据最外层的数组结构。取值范围：`TRUE` 或者 `FALSE`。默认值：`FALSE`。真实业务场景中，待导入的 JSON 数据可能在最外层有一对表示数组结构的中括号 `[]`。这种情况下，一般建议您指定该参数取值为 `true`，这样 StarRocks 会剪裁掉外层的中括号 `[]`，并把中括号 `[]` 里的每个内层数组都作为一行单独的数据导入。如果您指定该参数取值为 `false`，则 StarRocks 会把整个 JSON 数据解析成一个数组，并作为一行数据导入。例如，待导入的 JSON 数据为 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`，如果指定该参数取值为 `true`，则 StarRocks 会把 `{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 解析成两行数据，并导入到目标表中对应的数据行。 |
| jsonpaths                 | 否           | 用于指定待导入的字段的名称。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为 JSON 格式。参见[目标表存在衍生列，其列值通过表达式计算生成](#目标表存在衍生列其列值通过表达式计算生成)。|
| json_root                 | 否           | 如果不需要导入整个 JSON 数据，则指定实际待导入 JSON 数据的根节点。参数取值为合法的 JsonPath。默认值为空，表示会导入整个 JSON 数据。具体请参见本文提供的示例[指定实际待导入 JSON 数据的根节点](#指定实际待导入-json-数据的根节点)。 |
| task_consume_second                 | 否           |单个 Routine Load 导入作业中每个 Routine Load 导入任务消费数据的最大时长，单位为秒。相较于[FE 动态参数](../../../administration/Configuration.md) `routine_load_task_consume_second`（作用于集群内部所有 Routine Load 导入作业），该参数仅针对单个 Routine Load 导入作业，更加灵活。该参数自 v3.1.0 起新增。 <ul><li>当未配置 `task_consume_second` 和 `task_timeout_second` 时，StarRocks 则使用 FE 动态参数 `routine_load_task_consume_second` 和`routine_load_task_timeout_second` 来控制导入行为。</li><li>当只配置 `task_consume_second` 时，默认 `task_timeout_second` = `task_consume_second` * 4。</li><li>当只配置 `task_timeout_second` 时，默认 `task_consume_second` = `task_timeout_second` /4。</li></ul>|
| task_timeout_second                 | 否           | Routine Load 导入作业中每个 Routine Load 导入任务超时时间，单位为秒。相较于[FE 动态参数](../../../administration/Configuration.md) `routine_load_task_timeout_second`（作用于集群内部所有 Routine Load 导入作业），该参数仅针对单个 Routine Load 导入作业，更加灵活。该参数自 v3.1.0 起新增。<ul><li>当未配置 `task_consume_second` 和 `task_timeout_second` 时，StarRocks 则使用 FE 动态参数 `routine_load_task_consume_second` 和 `routine_load_task_timeout_second` 来控制导入行为。</li><li>当只配置 `task_timeout_second` 时，默认 `task_consume_second` = `task_timeout_second` /4。</li><li>当只配置 `task_consume_second` 时，默认 `task_timeout_second` = `task_consume_second` * 4。</li></ul>|

### `data_source`、`data_source_properties`

数据源和数据源属性。语法:

```Bash
FROM <data_source>
 ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

`data_source`

必填。指定数据源，目前仅支持取值为 `KAFKA`。

`data_source_properties`

必填。数据源属性，参数以及说明如下：

| **参数**          | **说明**                                                     |
| ----------------- | ------------------------------------------------------------ |
| kafka_broker_list | Kafka 的 Broker 连接信息。格式为 `<kafka_broker_ip>:<kafka port>`，多个 Broker 之间以英文逗号 (,) 分隔。 Kafka Broker 默认端口号为 `9092`。示例：`"kafka_broker_list" = "xxx.xx.xxx.xx:9092,xxx.xx.xxx.xx:9092"` |
| kafka_topic       | Kafka Topic 名称。一个导入作业仅支持消费一个 Topic 的消息。  |
| kafka_partitions  | 待消费的分区。示例：`"kafka_partitions" = "0, 1, 2, 3"`。如果不配置该参数，则默认消费所有分区。 |
| kafka_offsets     | 待消费分区的起始消费位点，必须一一对应 `kafka_partitions` 中指定的每个分区。如果不配置该参数，则默认为从分区的末尾开始消费。支持取值为<ul><li> 具体消费位点：从分区中该消费位点的数据开始消费。</li><li>`OFFSET_BEGINNING`：从分区中有数据的位置开始消费。</li><li>`OFFSET_END`：从分区的末尾开始消费。</li></ul>多个起始消费位点之间用英文逗号（, ）分隔。<br />示例： `"kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000"`。|
| property.kafka_default_offsets | 所有待消费分区的默认起始消费位点。支持的取值与 `kafka_offsets` 一致。 |
| confluent.schema.registry.url| 注册该 Avro schema 的 Schema Registry 的 URL，StarRocks 会从该 URL 获取 Avro schema。格式为 `confluent.schema.registry.url = http[s]://[<schema-registry-api-key>:<schema-registry-api-secret>@]<hostname or ip address>[:<port>]`。|

**更多数据源相关参数**

支持设置更多数据源 Kafka 相关参数，功能等同于 Kafka 命令行 `--property`， 支持参数，请参见 [librdkafka 配置项文档](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)中适用于客户端的配置项。

> **说明**
>
> 当参数的取值是文件时，则值前加上关键词 `FILE:`。关于如何创建文件，请参见 [CREATE FILE](../Administration/CREATE_FILE.md) 命令文档。

**指定所有待消费分区的默认起始消费位点。**

```Plaintext
"property.kafka_default_offsets" = "OFFSET_BEGINNING"
```

`property.kafka_default_offsets` 的取值为具体的消费位点，或者：

- `OFFSET_BEGINNING`：从分区中有数据的位置开始消费。
- `OFFSET_END`：从分区的末尾开始消费。

**指定导入任务消费 Kafka 时所基于 Consumer Group 的 `group.id`**

```Plaintext
"property.group.id" = "group_id_0"
```

如果没有指定 `group.id`，StarRocks 会根据 Routine Load 的导入作业名称生成一个随机值，具体格式为`{job_name}_{random uuid}`，如 `simple_job_0a64fe25-3983-44b2-a4d8-f52d3af4c3e8`。

**指定 BE 访问 Kafka 时的安全协议并配置相关参数**

支持安全协议为 `plaintext`（默认）、`ssl`、`sasl_plaintext` 和 `sasl_ssl`，并且需要根据安全协议配置相关参数。

当安全协议为 `sasl_plaintext` 或 `sasl_ssl` 时，支持如下 SASL 认证机制：

- PLAIN
- SCRAM-SHA-256 和 SCRAM-SHA-512
- OAUTHBEARER
- GSSAPI (Kerberos)

示例：

- 访问 Kafka 时，使用安全协议 SSL

    ```sql
    "property.security.protocol" = "ssl", -- 指定安全协议为 SSL
    "property.ssl.ca.location" = "FILE:ca-cert", -- CA 证书的位置
    --如果 Kafka server 端开启了 client 认证，则还需设置如下三个参数：
    "property.ssl.certificate.location" = "FILE:client.pem", -- Client 的 public key 的位置
    "property.ssl.key.location" = "FILE:client.key", -- Client 的 private key 的位置
    "property.ssl.key.password" = "abcdefg" -- Client 的 private key 的密码
    ```

- 访问 Kafka 时，使用 SASL_PLAINTEXT 安全协议和 SASL/PLAIN 认证机制

    ```sql
    "property.security.protocol" = "SASL_PLAINTEXT", -- 指定安全协议为 SASL_PLAINTEXT
    "property.sasl.mechanism" = "PLAIN", -- 指定 SASL 认证机制为 PLAIN
    "property.sasl.username" = "admin", -- SASL 的用户名
    "property.sasl.password" = "admin" -- SASL 的密码
    ```

- 访问 Kafka 时，使用 SASL_PLAINTEXT 安全协议和 SASL/GSSAPI (Kerberos) 认证机制

  ```sql
  "property.security.protocol" = "SASL_PLAINTEXT", -- 指定安全协议为 SASL_PLAINTEXT
  "property.sasl.mechanism" = "GSSAPI", -- 指定 SASL 认证机制为 GSSAPI, 默认是 GSSAPI
  "property.sasl.kerberos.service.name" = "kafka", -- 指定 broker service name，默认是 Kafka
  "property.sasl.kerberos.keytab" = "/home/starrocks/starrocks.keytab", -- 指定 client keytab 的位置
  "property.sasl.kerberos.principal" = "starrocks@YOUR.COM" -- 指定 kerberos principal
  ```

  :::note

  - 自 StarRocks 3.1.4 版本起，支持 SASL/GSSAPI (Kerberos) 认证。
  - 需要在 BE 机器上安装 SASL 相关模块。

    ```bash
    # Debian/Ubuntu:
    sudo apt-get install libsasl2-modules-gssapi-mit libsasl2-dev
    # CentOS/Redhat:
    sudo yum install cyrus-sasl-gssapi cyrus-sasl-devel
    ```

  :::

### FE 和 BE 配置项

Routine Load 相关配置项，请参见[配置参数](../../../administration/Configuration.md)。

## 列映射和转换关系

### 导入 CSV 数据

**如果 CSV 格式的数据中的列与目标表中的列的数量或顺序不一致**，则需要通过 `COLUMNS` 参数来指定源数据和目标表之间的列映射和转换关系。一般包括如下两种场景：

- **源数据中的列与目标表中的列数量一致，但是顺序不一致**。并且数据不需要通过函数计算、可以直接落入目标表中对应的列。
  您需要在 `COLUMNS` 参数中按照源数据中的列顺序、使用目标表中对应的列名来配置列映射和转换关系。

  例如，目标表中有三列，按顺序依次为 `col1`、`col2` 和 `col3`；源数据中也有三列，按顺序依次对应目标表中的 `col3`、`col2` 和 `col1`。这种情况下，需要指定 `COLUMNS(col3, col2, col1)`。
- **源数据中的列与目标表中的列数量不一致，甚至某些列的数据需要通过转换（函数计算以后）才能落入目标表中对应的列。**
  您不仅需要在 `COLUMNS` 参数中按照源数据中的列顺序、使用目标表中对应的列名来配置列映射关系，还需要指定参与数据计算的函数。以下为两个示例：

  - **源数据比目标表多列**。
    比如目标表中有三列，按顺序依次为 `col1`、`col2` 和 `col3` ；源数据中有四列，前三列按顺序依次对应目标表中的 `col1`、`col2` 和 `col3`，第四列在目标表中无对应的列。这种情况下，需要指定 `COLUMNS(col1, col2, col3, temp)`，其中，最后一列可随意指定一个名称（如 `temp`）用于占位即可。
  - **目标表存在基于源数据的列进行计算后生成的衍生列**。
    例如源数据中只有一个包含时间数据的列，格式为 `yyyy-mm-dd hh:mm:ss`。目标表中有三列，按顺序依次为 `year`、`month` 和 `day`，均是基于源数据中包含时间数据的列进行计算后生成的衍生列。这种情况下，可以指定 `COLUMNS(col, year = year(col), month=month(col), day=day(col)`。其中，`col` 是源数据中所包含的列的临时命名，`year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于指定从源数据中的 `col` 列提取对应的数据并落入目标表中对应的衍生列，如 `year = year(col)` 表示通过 `year` 函数提取源数据中 `col` 列的 `yyyy` 部分的数据并落入目标表中的 `year` 列。

  有关操作示例，请参见[设置列的映射和转换关系](#设置列的映射和转换关系)。

### 导入 JSON 或 Avro 数据

> **说明**
>
> 自 3.0.1 版本开始，StarRocks 支持使用 Routine Load 导入 Avro 数据。导入 JSON 或者 Avro 数据时，列映射和转换关系的配置方式相同。因此本小节以导入 Avro 数据为例进行说明。

**如果 JSON 格式的数据中的 Key 名与目标表中的列名不一致**，则需要使用匹配模式导入 JSON 数据，即通过 `jsonpaths` 和 `COLUMNS` 两个参数来指定源数据和目标表之间的列映射和转换关系：

- `jsonpaths` 参数指定待导入 JSON 数据的 Key，并进行排序（就像新生成了 CSV 数据）。
- `COLUMNS`参数指定待导入 JSON 数据的 Key 与目标表的列的映射关系和数据转换关系。
  - 与 `jsonpaths`中指定的 Key 按顺序保持一一对应。
  - 与目标表中的列按名称保持一一对应。

详细示例，请参见[目标表存在衍生列，其列值通过表达式计算生成](#目标表存在衍生列其列值通过表达式计算生成)。

> **说明**
>
> 如果待导入 JSON 数据中 Key 名（Key的顺序和数量不需要对应）都能对应目标表中列名，则可以使用简单模式导入 JSON 数据，无需配置 `jsonpaths` 和 `COLUMNS`。

## 示例

### 导入 CSV 数据

本小节以 CSV 格式的数据为例，重点阐述在创建导入作业的时候，如何运用各种参数配置来满足不同业务场景下的各种导入要求。

**数据集**

假设 Kafka 集群的 Topic `ordertest1` 存在如下 CSV 格式的数据，其中 CSV 数据中列的含义依次是订单编号、支付日期、顾客姓名、国籍、性别、支付金额。

```Plaintext
2020050802,2020-05-08,Johann Georg Faust,Deutschland,male,895
2020050802,2020-05-08,Julien Sorel,France,male,893
2020050803,2020-05-08,Dorian Grey,UK,male,1262
2020050901,2020-05-09,Anna Karenina,Russia,female,175
2020051001,2020-05-10,Tess Durbeyfield,US,female,986
2020051101,2020-05-11,Edogawa Conan,japan,male,8924
```

**目标数据库和表**

根据 CSV 数据中需要导入的列， 在 StarRocks 集群的目标数据库 `example_db` 中创建表 `example_tbl1`，建表语句如下：

```SQL
CREATE TABLE example_db.example_tbl1 ( 
    `order_id` bigint NOT NULL COMMENT "订单编号",
    `pay_dt` date NOT NULL COMMENT "支付日期", 
    `customer_name` varchar(26) NULL COMMENT "顾客姓名", 
    `nationality` varchar(26) NULL COMMENT "国籍", 
    `gender` varchar(26) NULL COMMENT "性别", 
    `price` double NULL COMMENT "支付金额") 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`);
```

#### 从 Topic 指定分区和起始位点开始消费

如果需要指定分区，以及各个分区对应的起始位点，则需要配置参数 `kafka_partitions`、`kafka_offsets`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    "kafka_partitions" ="0,1,2,3", -- 指定分区
    "kafka_offsets" = "1000, OFFSET_BEGINNING, OFFSET_END, 2000" -- 指定起始位点
);
```

#### 调整导入性能

如果需要提高导入性能，避免出现消费积压等情况，则可以通过设置单个 Routine Load 导入作业的期望任务并发度`desired_concurrent_number`，增加实际任务并行度，将一个导入作业拆分成尽可能多的导入任务并行执行。

> 更多提升导入性能的方式，请参见 [Routine Load常见问题](../../../faq/loading/Routine_load_faq.md)。

请注意，实际任务并行度由如下多个参数组成的公式决定，上限为 BE 节点的数量或者消费分区的数量。

```SQL
min(alive_be_number, partition_number, desired_concurrent_number, max_routine_load_task_concurrent_num)
```

因此当消费分区和 BE 节点数量较多，并且大于其余两个参数时，如果您需要增加实际任务并行度，则可以提高`max_routine_load_task_concurrent_num`、 `desired_concurrent_number` 的值。

假设消费分区数量为 `7`，存活 BE 数量为 `5`，`max_routine_load_task_concurrent_num` 为默认值 `5`。此时如果需要增加实际任务并发度至上限，则需要将 `desired_concurrent_number` 设置为 `5`（默认值为 `3`），则计算实际任务并行度 `min(5,7,5,5)` 为 `5`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"desired_concurrent_number" = "5" -- 设置单个 Routine Load 导入作业的期望任务并发度
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 设置列的映射和转换关系

如果 CSV 格式的数据中的列与目标表中的列的数量或顺序不一致，假设无需导入 CSV 数据的第五列至目标表，则需要通过 `COLUMNS` 参数来指定源数据和目标表之间的列映射和转换关系。

**目标数据库和表**

根据 CSV 数据中需要导入的几列（例如除第五列性别外的其余五列需要导入至 StarRocks）， 在 StarRocks 集群的目标数据库 `example_db` 中创建表 `example_tbl2`。

```SQL
CREATE TABLE example_db.example_tbl2 ( 
    `order_id` bigint NOT NULL COMMENT "订单编号",
    `pay_dt` date NOT NULL COMMENT "支付日期", 
    `customer_name` varchar(26) NULL COMMENT "顾客姓名", 
    `nationality` varchar(26) NULL COMMENT "国籍", 
    `price` double NULL COMMENT "支付金额"
) 
DUPLICATE KEY (order_id,pay_dt) 
DISTRIBUTED BY HASH(`order_id`);
```

**导入作业**

本示例中，由于无需导入 CSV 数据的第五列至目标表，因此`COLUMNS`中把第五列临时命名为 `temp_gender` 用于占位，其他列都直接映射至表 `example_tbl2` 中。

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

#### 设置过滤条件 筛选待导入的数据

如果仅导入满足条件的数据，则可以在 WHERE 子句中设置过滤条件，例如`price > 100`。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price),
WHERE price > 100
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 设置导入任务为严格模式

在`PROPERTIES`中设置`"strict_mode" = "true"`，表示导入作业为严格模式。如果源数据某列的值为 NULL，但是目标表中该列不允许为 NULL，则该行数据会被过滤掉。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"strict_mode" = "true" -- 设置导入作业为严格模式
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 设置导入任务的容错率

如果业务场景对数据质量的有要求，则需要设置参数`max_batch_rows`和`max_error_number`设置错误检测窗口的范围和允许的错误数据行数的上限，当错误数据行数超过该值时，导入作业会暂停。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"max_batch_rows" = "100000", -- 错误检测窗口范围为单个 Routine Load 导入任务所消费的 10 * max-batch-rows 行数。
"max_error_number" = "100" -- 错误检测窗口范围内允许的错误数据行数的上限
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1"
);
```

#### 指定安全协议为 SSL 并配置相关参数

如果需要指定 BE 访问 Kafka 时使用的安全协议为 SSL，则需要配置 `"property.security.protocol" = "ssl"` 等参数。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl1_ordertest1 ON example_tbl1
COLUMNS TERMINATED BY ",",
COLUMNS (order_id, pay_dt, customer_name, nationality, gender, price)
PROPERTIES
(
"desired_concurrent_number" = "5"
)
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest1",
    "property.security.protocol" = "ssl", -- 使用 SSL 加密
    "property.ssl.ca.location" = "FILE:ca-cert", -- CA 证书的位置
    -- 如果 Kafka Server 端开启了 Client 身份认证，则还需设置如下三个参数：
    "property.ssl.certificate.location" = "FILE:client.pem", -- Client 的 Public Key 的位置
    "property.ssl.key.location" = "FILE:client.key", -- Client 的 Private Key 的位置
    "property.ssl.key.password" = "abcdefg" -- Client 的 Private Key 的密码
);
```

#### 设置 `trim_space`、`enclose` 和 `escape`

假设 Kafka 集群的 Topic `test_csv` 存在如下 CSV 格式的数据，其中 CSV 数据中列的含义依次是订单编号、支付日期、顾客姓名、国籍、性别、支付金额。

```Plain
 "2020050802" , "2020-05-08" , "Johann Georg Faust" , "Deutschland" , "male" , "895"
 "2020050802" , "2020-05-08" , "Julien Sorel" , "France" , "male" , "893"
 "2020050803" , "2020-05-08" , "Dorian Grey\,Lord Henry" , "UK" , "male" , "1262"
 "2020050901" , "2020-05-09" , "Anna Karenina" , "Russia" , "female" , "175"
 "2020051001" , "2020-05-10" , "Tess Durbeyfield" , "US" , "female" , "986"
 "2020051101" , "2020-05-11" , "Edogawa Conan" , "japan" , "male" , "8924"
```

如果要把 Topic `test_csv` 中所有的数据都导入到 `example_tbl1` 中，并且希望去除被 `enclose` 指定字符括起来的字段前后的空格、 指定 `enclose` 字符为双引号 (")、并且指定 `escape` 字符为斜杠 (`\`)，可以执行如下语句：

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

#### 目标表的列名与 JSON 数据的 Key 一致

可以使用简单模式导入数据，即创建导入作业时无需使用 `jsonpaths` 和 `COLUMNS` 参数。StarRocks 会按照目标表的列名去对应 JSON 数据的 Key。

**数据集**

假设 Kafka 集群的 Topic `ordertest2` 中存在如下 JSON 数据。

```JSON
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

> 注意
>
> 这里每行一个 JSON 对象必须在一个 Kafka 消息中，否则会出现“JSON 解析错误”的问题。

**目标数据库和表**

在 StarRocks 集群的目标数据库 `example_db` 中创建表 `example_tbl3` ，并且列名与 JSON 数据中需要导入的 Key 一致。

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    `commodity_id` varchar(26) NULL COMMENT "品类ID", 
    `customer_name` varchar(26) NULL COMMENT "顾客姓名", 
    `country` varchar(26) NULL COMMENT "顾客国籍", 
    `pay_time` bigint(20) NULL COMMENT "支付时间", 
    `price` double SUM NULL COMMENT "支付金额") 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`) 
DISTRIBUTED BY HASH(`commodity_id`);
```

**导入作业**

提交导入作业时使用简单模式，即无需使用`jsonpaths` 和 `COLUMNS` 参数，就可以将 Kafka 集群的 Topic `ordertest2` 中的 JSON 数据导入至目标表 `example_tbl3` 中。

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

> **说明**
>
> - 如果 JSON 数据最外层是数组结构，则需要在`PROPERTIES`设置`"strip_outer_array"="true"`，表示裁剪最外层的数组结构。并且需要注意在设置 `jsonpaths` 时，整个 JSON 数据的根节点是裁剪最外层的数组结构后**展平的 JSON 对象**。
> - 如果不需要导入整个 JSON 数据，则需要使用 `json_root` 指定实际所需导入的 JSON 数据根节点。

#### 目标表存在衍生列，其列值通过表达式计算生成

需要使用匹配模式导入数据，即需要使用 `jsonpaths` 和 `COLUMNS` 参数，`jsonpaths`指定待导入 JSON 数据的 Key，`COLUMNS` 参数指定待导入 JSON 数据的 Key 与目标表的列的映射关系和数据转换关系。

**数据集**

假设 Kafka 集群的 Topic `ordertest2` 中存在如下 JSON 格式的数据。

```JSON
{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875}
{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895}
{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}
```

**目标数据库和表**

假设在 StarRocks 集群的目标数据库 `example_db` 中存在目标表 `example_tbl4` 其中有一列衍生列 `pay_dt`，是基于 JSON 数据的Key `pay_time` 进行计算后的数据。其建表语句如下：

```SQL
CREATE TABLE example_db.example_tbl4 ( 
    `commodity_id` varchar(26) NULL COMMENT "品类ID", 
    `customer_name` varchar(26) NULL COMMENT "顾客姓名", 
    `country` varchar(26) NULL COMMENT "顾客国籍",
    `pay_time` bigint(20) NULL COMMENT "支付时间",  
    `pay_dt` date NULL COMMENT "支付日期", 
    `price`double SUM NULL COMMENT "支付金额") 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`,`pay_dt`) 
DISTRIBUTED BY HASH(`commodity_id`);
```

**导入作业**

提交导入作业时使用匹配模式。使用 `jsonpaths` 指定待导入 JSON 数据的 Key。并且由于 JSON 数据中 key `pay_time` 需要转换为 DATE 类型，才能导入到目标表的列 `pay_dt`，因此 `COLUMNS` 中需要使用函数`from_unixtime`进行转换。JSON 数据的其他 key 都能直接映射至表 `example_tbl4` 中。

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

> **说明**
>
> - 如果 JSON 数据最外层是数组结构，则需要在`PROPERTIES`设置`"strip_outer_array"="true"`，表示裁剪最外层的数组结构。并且需要注意在设置 `jsonpaths` 时，整个 JSON 数据的根节点是裁剪最外层的数组结构后**展平的 JSON 对象**。
> - 如果不需要导入整个 JSON 数据，则需要使用 `json_root` 指定实际所需导入的 JSON 数据根节点。

#### 目标表存在衍生列，其列值通过 CASE 表达式计算生成

**数据集**

假设 Kafka 集群的 Topic `topic-expr-test` 中存在如下 JSON 格式的数据。

```JSON
{"key1":1, "key2": 21}
{"key1":12, "key2": 22}
{"key1":13, "key2": 23}
{"key1":14, "key2": 24}
```

**目标数据库和表**

假设在 StarRocks 集群的目标数据库 `example_db` 中存在目标表 `tbl_expr_test` 包含两列，其中列 `col2` 的值基于 JSON 数据进行 CASE 表达式计算得出。其建表语句如下：

```SQL
CREATE TABLE tbl_expr_test (
    col1 string, col2 string)
DISTRIBUTED BY HASH (col1);
```

**导入作业**

目标表中列 `col2` 的值需要基于 JSON 数据进行 CASE 表达式计算后得出，因此您需要在导入作业中的 `COLUMNS` 参数配置对应的 CASE 表达式。

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

**查询数据**

查询目标表中的数据，返回结果显示列 `col2` 的值是使用 CASE 表达式计算后输出的值。

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

#### 指定实际待导入 JSON 数据的根节点

如果不需要导入整个 JSON 数据，则需要使用 `json_root` 指定实际上所需导入的 JSON 数据的根对象，参数取值为合法的 JsonPath。

**数据集**

假设 Kafka 集群的 Topic `ordertest3` 中存在如下 JSON 格式的数据，实际导入时仅需要导入 key `RECORDS`的值。

```JSON
{"RECORDS":[{"commodity_id": "1", "customer_name": "Mark Twain", "country": "US","pay_time": 1589191487,"price": 875},{"commodity_id": "2", "customer_name": "Oscar Wilde", "country": "UK","pay_time": 1589191487,"price": 895},{"commodity_id": "3", "customer_name": "Antoine de Saint-Exupéry","country": "France","pay_time": 1589191487,"price": 895}]}
```

**目标数据库和表**

假设在 StarRocks 集群的目标数据库 `example_db` 中存在目标表`example_tbl3` ，其建表语句如下：

```SQL
CREATE TABLE example_db.example_tbl3 ( 
    `commodity_id` varchar(26) NULL COMMENT "品类ID", 
    `customer_name` varchar(26) NULL COMMENT "顾客姓名", 
    `country` varchar(26) NULL COMMENT "顾客国籍", 
    `pay_time` bigint(20) NULL COMMENT "支付时间", 
    `price`double SUM NULL COMMENT "支付金额") 
AGGREGATE KEY(`commodity_id`,`customer_name`,`country`,`pay_time`) 
DISTRIBUTED BY HASH(`commodity_id`);
```

**导入作业**

提交导入作业，设置`"json_root" = "$.RECORDS"`指定实际待导入的 JSON 数据的根节点。并且由于实际待导入的 JSON 数据是数组结构，因此还需要设置`"strip_outer_array" = "true"`，裁剪外层的数组结构。

```SQL
CREATE ROUTINE LOAD example_db.example_tbl3_ordertest3 ON example_tbl3
PROPERTIES
(
    "format" = "json",
    "strip_outer_array" = "true",
    "json_root" = "$.RECORDS"
 )
FROM KAFKA
(
    "kafka_broker_list" ="<kafka_broker1_ip>:<kafka_broker1_port>,<kafka_broker2_ip>:<kafka_broker2_port>",
    "kafka_topic" = "ordertest2"
);
```

### 导入 Avro 数据

自 3.0.1 版本开始，StarRocks 支持使用 Routine Load 导入 Avro 数据。

#### Avro schema 只包含简单数据类型

假设 Avro schema 只包含简单数据类型，并且您需要导入 Avro 数据中的所有字段。

**数据集**

**Avro schema**

1. 创建如下 Avro schema 文件 `avro_schema1.avsc`：

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

2. 注册该 Avro schema 至 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)。

**Avro 数据**

构建 Avro 数据并且发送至 Kafka 集群的 topic `topic_1`。

**目标数据库和表**

根据 Avro 数据中需要导入的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log1`。表的列名与 Avro 数据的字段名保持一致。两者的数据类型映射关系，请参见xxx。

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

**导入作业**

提交导入作业时使用简单模式，即无需使用 `jsonpaths` 参数，就可以将 Kafka 集群的 Topic `topic_1` 中的 Avro 数据导入至数据库 `sensor` 中的表 `sensor_log1`。

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
  "kafka_topic" = "topic_1",  
  "kafka_partitions" = "0,1,2,3,4,5",  
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"  
);
```

#### Avro schema 嵌套 Record 类型的字段

假设 Avro schema 嵌套 Record 类型的字段，并且您需要导入嵌套 Record 字段中的子字段。

**数据集**

**Avro schema**

1. 创建如下 Avro schema 文件 `avro_schema2.avsc`。其中最外层 Record 的字段依次是 `id`、 `name`、`checked`、`sensor_type` 和 `data`。并且字段 `data` 包含嵌套 Record `data_record`。

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

2. 注册该 Avro schema 至 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)。

**Avro 数据**

构建 Avro 数据并且发送至 Kafka 集群的 topic `topic_2`。

**目标数据库和表**

根据 Avro 数据中需要导入的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log2`。

假设您除了需要导入最外层 Record 的 `id`、`name`、`checked`、`sensor_type` 字段之外，还需要导入嵌套 Record 中的字段 `data_y`。

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

**导入作业**

提交导入作业，使用 `jsonpaths` 指定实际待导入的 Avro 数据的字段。其中对于嵌套 Record 中的字段 `data_y`，您需要指定 `jsonpaths` 为 `"$.data.data_y"`。

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

#### Avro schema 包含 Union 类型的字段

**数据集**

假设 Avro 数据包含 Union 类型的字段，并且您需要导入该 Union 字段。

**Avro schema**

1. 创建如下 Avro schema 文件 `avro_schema3.avsc`。其中最外层 Record 的字段依次是`id`、`name`、 `checked`、 `sensor_type` 和 `data`。并且字段 `data`为 Union 类型，包含两个元素，分别是 `null` 和嵌套 Record `data_record` 。

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

2. 注册该 Avro schema 至 [Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)。

**Avro 数据**

构建 Avro 数据并且发送至 Kafka 集群的 topic `topic_3`。

**目标数据库和表**

根据 Avro 数据中需要导入的字段，在 StarRocks 集群的目标数据库 `sensor` 中创建表 `sensor_log3` 。

假设您除了需要导入最外层 Record 的 `id`、`name`、`checked`、`sensor_type` 字段之外，还需要导入 Union 类型的字段 `data` 中元素 `data_record` 包含的字段 `data_y`。

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

**导入作业**

提交导入作业，使用 `jsonpaths` 指定实际待导入的 Avro 数据的字段。其中您需要指定字段 `data_y`的`jsonpaths` 为 `"$.data.data_y"`。

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

当 Union 类型的字段 `data` 的值为 `null` 时，则导入 `data_y` 列的值为 `null`。当 `data` 的值为一条 data record 时，则导入  `data_y` 列的值 Long 类型。
