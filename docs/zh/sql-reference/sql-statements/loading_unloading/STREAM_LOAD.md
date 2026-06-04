---
displayed_sidebar: docs
toc_max_heading_level: 4
description: "STREAM LOAD 允许您从本地文件系统或流式数据源加载数据。"
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';
import TableURL from '../../../_assets/commonMarkdown/stream_load_table_url.mdx';
import TableURLTip from '../../../_assets/commonMarkdown/stream_load_table_url_tip.mdx';

# STREAM LOAD

STREAM LOAD 允许您从本地文件系统或流式数据源加载数据。提交加载作业后，系统会同步运行该作业，并在作业完成后返回作业结果。您可以根据作业结果判断作业是否成功。有关 Stream Load 的应用场景、限制和支持的数据文件格式的信息，请参阅[通过 Stream Load 从本地文件系统加载](../../../loading/StreamLoad.md)。

<Tip />

自 v3.2.7 起，Stream Load 支持在传输过程中压缩 JSON 数据，从而降低网络带宽开销。用户可以使用参数 `compression` 和 `Content-Encoding` 指定不同的压缩算法。支持的压缩算法包括 GZIP、BZIP2、LZ4_FRAME 和 ZSTD。更多信息，请参阅[data_desc](#data_desc)。

自 v3.4.0 起，系统支持合并多个 Stream Load 请求。更多信息，请参阅[Merge Commit 参数](#merge-commit-parameters)。

:::note

- 使用 Stream Load 将数据加载到原生表后，在该表上创建的物化视图的数据也会同步更新。
- 您只能以对目标表拥有 INSERT 权限的用户身份将数据加载到原生表中。如果您没有 INSERT 权限，请按照[GRANT](../account-management/GRANT.md)中的说明将 INSERT 权限授予您用于连接集群的用户。
:::

## 语法

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

本主题以 `curl` 为例，介绍如何使用 Stream Load 加载数据。除 `curl` 外，您还可以使用其他兼容 HTTP 的工具或语言执行 Stream Load。与加载相关的参数包含在 HTTP 请求头字段中。输入这些参数时，请注意以下几点：

- 您可以使用分块传输编码，如本主题所示。如果您不选择分块传输编码，则必须输入 `Content-Length` 头字段以指示要传输的内容长度，从而确保数据完整性。

  :::note
  如果您使用 `curl` 执行 Stream Load，系统会自动添加 `Content-Length` 头字段，您无需手动输入。
  :::

- 您必须添加 `Expect` 头字段并将其值指定为 `100-continue`，如 `"Expect:100-continue"`。这有助于在您的作业请求被拒绝时防止不必要的数据传输并减少资源开销。

请注意，在 StarRocks 中，某些字面量被 SQL 语言用作保留关键字。请勿在 SQL 语句中直接使用这些关键字。如果您想在 SQL 语句中使用此类关键字，请将其用反引号（`）括起来。请参阅[关键字](../keywords.md)。

## 参数

### username 和 password

指定您用于连接集群的账户的用户名和密码。这是必填参数。如果您使用未设置密码的账户，则只需输入 `<username>:`。

### XPUT

指定 HTTP 请求方法。这是必填参数。Stream Load 仅支持 PUT 方法。

### url

指定表的 URL。语法：

<TableURL />

### data_desc

描述您要加载的数据文件。`data_desc` 描述符可以包含数据文件的名称、格式、列分隔符、行分隔符、目标分区以及与表的列映射关系。语法：

```Bash
-T <file_path>
-H "format: CSV | JSON"
-H "column_separator: <column_separator>"
-H "row_delimiter: <row_delimiter>"
-H "columns: <column1_name>[, <column2_name>, ... ]"
-H "partitions: <partition1_name>[, <partition2_name>, ...]"
-H "temporary_partitions: <temporary_partition1_name>[, <temporary_partition2_name>, ...]"
-H "jsonpaths: [ \"<json_path1>\"[, \"<json_path2>\", ...] ]"
-H "strip_outer_array: true | false"
-H "json_root: <json_path>"
-H "ignore_json_size: true | false"
-H "compression: <compression_algorithm> | Content-Encoding: <compression_algorithm>"
```

`data_desc` 描述符中的参数可分为三类：通用参数、CSV 参数和 JSON 参数。

#### 通用参数

| 参数 | 是否必填 | 描述 |
| ---------- | -------- | ------------------------------------------------------------ |
| file_path | 是 | 数据文件的保存路径。您可以选择性地包含文件名的扩展名。|
| format | 否 | 数据文件的格式。有效值：`CSV` 和 `JSON`。默认值：`CSV`。|
| partitions | 否 | 您要将数据文件加载到的分区。默认情况下，如果不指定此参数，系统会将数据文件加载到表的所有分区中。|
| temporary_partitions | 否 | 您要将数据文件加载到的[临时分区](../../../table_design/data_distribution/Temporary_partition.md)的名称。您可以指定多个临时分区，多个分区之间必须用逗号（,）分隔。|
| columns | 否 | 数据文件与表之间的列映射关系。<br />如果数据文件中的字段可以按顺序映射到表中的列，则无需指定此参数。您可以使用此参数实现数据转换。例如，如果您加载一个 CSV 数据文件，该文件由两列组成，可以按顺序映射到表的两列 `id` 和 `city`，则可以指定 `"columns: city,tmp_id, id = tmp_id * 100"`。更多信息，请参阅本主题中的「[列映射](#column-mapping)」部分。|

#### CSV 参数

##### `column_separator`

必填：否

描述：数据文件中用于分隔字段的字符。如果不指定此参数，则默认为 `\t`，表示制表符。<br />请确保通过此参数指定的列分隔符与数据文件中使用的列分隔符相同。<br />**注意**<br />- 对于 CSV 数据，您可以使用 UTF-8 字符串（例如逗号 (,)、制表符或竖线 (|)），长度不超过 50 字节，作为文本分隔符。<br />- 如果数据文件使用连续的不可打印字符（例如 `\r\n`）作为列分隔符，则必须将此参数设置为 `\\x0D0A`。

##### `row_delimiter`

必填：否

描述：数据文件中用于分隔行的字符。如果不指定此参数，则默认为 `\n`。<br />**注意**<br />如果数据文件使用连续的不可打印字符（例如 `\r\n`）作为行分隔符，则必须将此参数设置为 `\\x0D0A`。

##### `skip_header`

必填：否

描述：指定当数据文件为 CSV 格式时，是否跳过数据文件开头的若干行。类型：INTEGER。默认值：`0`。<br />在某些 CSV 格式的数据文件中，开头的若干行用于定义元数据，例如列名和列数据类型。通过设置 `skip_header` 参数，您可以让系统在数据导入时跳过数据文件的开头若干行。例如，如果将此参数设置为 `1`，系统将在数据导入时跳过数据文件的第一行。<br />数据文件开头的若干行必须使用您在导入命令中指定的行分隔符进行分隔。

##### `trim_space`

必填：否
描述：指定当数据文件为 CSV 格式时，是否去除数据文件中列分隔符前后的空格。类型：BOOLEAN。默认值：`false`。<br />某些数据库在将数据导出为 CSV 格式的数据文件时，会在列分隔符处添加空格。这些空格根据其位置被称为前导空格或尾随空格。通过设置 `trim_space` 参数，您可以让系统在数据导入时去除这些多余的空格。<br />请注意，系统不会去除由 `enclose` 指定字符包裹的字段内部的空格（包括前导空格和尾随空格）。例如，以下字段值使用竖线（`|`）作为列分隔符，使用双引号（`"`）作为 `enclose` 指定的字符：<br />`|"Love StarRocks"|` <br />`|" Love StarRocks "|` <br />`| "Love StarRocks" |` <br />如果将 `trim_space` 设置为 `true`，系统将按如下方式处理上述字段值：<br />`|"Love StarRocks"|` <br />`|" Love StarRocks "|` <br />`|"Love StarRocks"|`

##### `enclose`

必填：否

描述：根据[RFC4180](https://www.rfc-editor.org/rfc/rfc4180)指定当数据文件为 CSV 格式时，用于包裹数据文件中字段值的字符。类型：单字节字符。默认值：`NONE`。最常用的字符为单引号（`'`）和双引号（`"`）。<br />所有由 `enclose` 指定字符包裹的特殊字符（包括行分隔符和列分隔符）均被视为普通符号。系统的功能超越了 RFC4180，允许您将任意单字节字符指定为 `enclose` 指定的字符。<br />如果字段值中包含 `enclose` 指定的字符，您可以使用相同的字符对该 `enclose` 指定的字符进行转义。例如，将 `enclose` 设置为 `"`，且字段值为 `a "quoted" c`。在这种情况下，您可以在数据文件中将该字段值输入为 `"a ""quoted"" c"`。|

##### `escape`

必填：否

描述：指定用于转义各种特殊字符（例如行分隔符、列分隔符、转义字符以及 `enclose` 指定的字符）的字符，这些特殊字符将被视为普通字符，并作为其所在字段值的一部分进行解析。类型：单字节字符。默认值：`NONE`。最常用的字符为斜杠（`\`），在 SQL 语句中必须写成双斜杠（`\\`）。<br />**注意**<br />`escape` 指定的字符在每对 `enclose` 指定字符的内部和外部均适用。<br />以下是两个示例：<ul><li>当将 `enclose` 设置为 `"`，并将 `escape` 设置为 `\` 时，系统将 `"say \"Hello world\""` 解析为 `say "Hello world"`。</li><li>假设列分隔符为逗号（`,`）。当将 `escape` 设置为 `\` 时，系统将 `a, b\, c` 解析为两个独立的字段值：`a` 和 `b, c`。</li></ul>

:::note

- 对于 CSV 数据，您可以使用 UTF-8 字符串（例如逗号 (,)、制表符或竖线 (|)），长度不超过 50 字节，作为文本分隔符。
- 空值使用 `\N` 表示。例如，一个数据文件包含三列，其中某条记录在第一列和第三列有数据，但第二列没有数据。在这种情况下，需要在第二列使用 `\N` 表示空值。这意味着该记录必须编写为 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示该记录的第二列存储的是空字符串。
- 格式选项，包括 `skip_header`、`trim_space`、`enclose` 和 `escape`，在 v3.0 及更高版本中受支持。
:::

#### JSON 参数

<<<<<<< HEAD
| **参数名称**      | **是否必选** | **参数说明**                                                 |
| ----------------- | ------------ | ------------------------------------------------------------ |
| jsonpaths         | 否           | 用于指定待导入的字段的名称。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为 JSON 格式。参见[导入 JSON 数据时配置列映射关系](#导入-json-数据时配置列映射关系)。    |
| strip_outer_array | 否           | 用于指定是否裁剪最外层的数组结构。取值范围：`true` 和 `false`。默认值：`false`。真实业务场景中，待导入的 JSON 数据可能在最外层有一对表示数组结构的中括号 `[]`。这种情况下，一般建议您指定该参数取值为 `true`，这样系统会剪裁掉外层的中括号 `[]`，并把中括号 `[]` 里的每个内层数组都作为一行单独的数据导入。如果您指定该参数取值为 `false`，则系统会把整个 JSON 数据文件解析成一个数组，并作为一行数据导入。例如，待导入的 JSON 数据为 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`，如果指定该参数取值为 `true`，则系统会把 `{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 解析成两行数据，并导入到目标表中对应的数据行。 |
| json_root         | 否           | 用于指定待导入 JSON 数据的根元素。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为合法的 JsonPath 字符串。默认值为空，表示会导入整个 JSON 数据文件的数据。具体请参见本文提供的示例“[导入数据并指定 JSON 根节点](#指定-json-根节点使用匹配模式导入数据)”。 |
| ignore_json_size | 否   | 用于指定是否检查 HTTP 请求中 JSON Body 的大小。<br />**说明**<br />HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。 |
| compression, Content-Encoding | 否 | 指定在 STREAM LOAD 数据传输过程中使用哪种压缩算法，支持 GZIP、BZIP2、LZ4_FRAME、ZSTD 算法。示例：`curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。 |
=======
| 参数              | 必填 | 描述                                                         |
| ----------------- | ---- | ------------------------------------------------------------ |
| jsonpaths         | 否   | 您希望从 JSON 数据文件中导入的键名。仅在使用匹配模式导入 JSON 数据时需要指定此参数。该参数的值为 JSON 格式。请参阅[为 JSON 数据加载配置列映射](#configure-column-mapping-for-json-data-loading)。           |
| strip_outer_array | 否   | 指定是否去除最外层的数组结构。有效值：`true` 和 `false`。默认值：`false`。<br />在实际业务场景中，JSON 数据可能具有由一对方括号 `[]` 表示的最外层数组结构。在这种情况下，建议将此参数设置为 `true`，以便系统去除最外层的方括号 `[]`，并将每个内部数组作为单独的数据记录导入。如果将此参数设置为 `false`，系统将把整个 JSON 数据文件解析为一个数组，并将该数组作为单条数据记录导入。<br />例如，JSON 数据为 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`。如果将此参数设置为 `true`，`{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 将被解析为单独的数据记录，并加载到不同的表行中。|
| json_root         | 否       | 您希望从 JSON 数据文件中加载的 JSON 数据的根元素。仅当使用匹配模式加载 JSON 数据时，才需要指定此参数。该参数的值为有效的 JsonPath 字符串。默认情况下，该参数值为空，表示将加载 JSON 数据文件的所有数据。更多信息，请参阅本主题的「[使用指定根元素的匹配模式加载 JSON 数据](#load-json-data-using-matched-mode-with-root-element-specified)」部分。|
| envelope          | 否       | 指定 JSON 数据的 CDC envelope 格式。有效值：`debezium`。默认值：未设置（无 envelope 包装）。当设置为 `debezium` 时，StarRocks 将每条 JSON 消息解析为 Debezium CDC 事件。消息必须包含一个 `op` 字段（`c`=创建，`u`=更新，`d`=删除，`r`=快照读取）以及一个 `after` 字段（用于 c/u/r）或 `before` 字段（用于 d），用于保存实际行数据。`payload` 为 `null` 的墓碑消息将被静默跳过。不能与 `json_root` 或 `strip_outer_array` 同时使用。|
| ignore_json_size  | 否       | 指定是否检查 HTTP 请求中 JSON 正文的大小。<br />**注意**<br />默认情况下，HTTP 请求中 JSON 正文的大小不能超过 100 MB。如果 JSON 正文超过 100 MB，将报告错误「The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming.」。为避免此错误，您可以在 HTTP 请求头中添加 `"ignore_json_size:true"`，以指示系统不检查 JSON 正文大小。|
| compression, Content-Encoding | 否 | 数据传输过程中应用的编码算法。支持的算法包括 GZIP、BZIP2、LZ4_FRAME 和 ZSTD。示例：`curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。|
>>>>>>> e16d27a64e ([Doc] generate descriptions (#74345))

加载 JSON 数据时，还需注意每个 JSON 对象的大小不能超过 4 GB。如果 JSON 数据文件中某个 JSON 对象超过 4 GB，将报告错误「This parser can't support a document that big.」。

### Merge Commit 参数

在指定时间窗口内，为多个并发 Stream Load 请求启用 Merge Commit，并将其合并为单个事务。

:::warning

请注意，Merge Commit 优化适用于**并发** 在单张表上并发执行 Stream Load 作业的场景。如果并发度为 1，则不建议使用。同时，在将 `merge_commit_async` 设置为 `false` 以及将 `merge_commit_interval_ms` 设置为较大值之前，请慎重考虑，因为这可能导致加载性能下降。

:::

| **参数**            | **是否必填** | **描述**                                              |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| enable_merge_commit      | 否           | 是否为加载请求启用 Merge Commit。有效值：`true` 和 `false`（默认值）。|
| merge_commit_async       | 否           | 服务器的返回模式。有效值：<ul><li>`true`：启用异步模式，服务器在接收到数据后立即返回。此模式不保证加载成功。</li><li>`false`（默认值）：启用同步模式，服务器仅在合并事务提交后才返回，确保加载成功且数据可见。</li></ul> |
| merge_commit_interval_ms | 是          | 合并时间窗口的大小。单位：毫秒。Merge Commit 尝试将在此窗口内收到的加载请求合并为单个事务。较大的窗口可提高合并效率，但会增加延迟。|
| merge_commit_parallel    | 是          | 为每个合并窗口创建的加载计划的并行度。可根据摄取负载调整并行度。如果请求数量较多和/或需要加载大量数据，请增大此值。并行度受 BE 节点数量限制，计算方式为 `min(merge_commit_parallel, number of BE nodes)`。|

:::note

- Merge Commit 仅支持将**同构** 加载请求合并到单个数据库和表中。「同构」表示 Stream Load 参数完全相同，包括：公共参数、JSON 格式参数、CSV 格式参数、`opt_properties` 以及 Merge Commit 参数。
- 加载 CSV 格式数据时，必须确保每行以行分隔符结尾。不支持 `skip_header`。
- 服务器自动为事务生成标签。如果指定了标签，将被忽略。
- Merge Commit 将多个加载请求合并为单个事务。如果某个请求包含数据质量问题，则事务中的所有请求都将失败。

:::

### opt_properties

指定一些可选参数，这些参数适用于整个加载作业。语法：

```Bash
-H "label: <label_name>"
-H "where: <condition1>[, <condition2>, ...]"
-H "max_filter_ratio: <num>"
-H "timeout: <num>"
-H "strict_mode: true | false"
-H "timezone: <string>"
-H "load_mem_limit: <num>"
-H "partial_update: true | false"
-H "partial_update_mode: row | column"
-H "merge_condition: <column_name>"
```

下表描述了可选参数。

| 参数        | 是否必填 | 描述                                                  |
| ---------------- | -------- | ------------------------------------------------------------ |
| label            | 否       | 加载作业的标签。如果不指定此参数，系统将自动为加载作业生成标签。<br />系统不允许使用同一标签多次加载同一批数据。因此，系统可防止相同数据被重复加载。有关标签命名规范，请参阅[系统限制](../../System_limit.md)。<br />默认情况下，系统保留最近三天内成功完成的加载作业的标签。您可以使用[FE 参数](../../../administration/management/FE_configuration.md) `label_keep_max_second` 更改标签保留期。|
| where            | 否       | 系统用于过滤预处理数据的条件。系统仅加载满足 WHERE 子句中指定过滤条件的预处理数据。|
| max_filter_ratio | 否       | 加载作业的最大错误容忍度。错误容忍度是指在加载作业请求的所有数据记录中，因数据质量不足而被过滤掉的数据记录的最大百分比。有效值：`0` 到 `1`。默认值：`0`。<br />我们建议您保留默认值 `0`。这样，如果检测到不合格的数据记录，加载作业将失败，从而确保数据正确性。<br />如果您想忽略不合格的数据记录，可以将此参数设置为大于 `0` 的值。这样，即使数据文件包含不合格的数据记录，加载作业也可以成功。<br />**注意**<br />不合格的数据记录不包括被 WHERE 子句过滤掉的数据记录。|
| log_rejected_record_num | 否 | 指定可以记录的不合格数据行的最大数量。此参数从 v3.1 版本开始支持。有效值：`0`、`-1` 以及任意非零正整数。默认值：`0`。<ul><li>值 `0` 表示被过滤掉的数据行将不会被记录。</li><li>值 `-1` 表示所有被过滤掉的数据行都将被记录。</li><li>非零正整数（如 `n`）表示每个 BE 或 CN 上最多可记录 `n` 行被过滤掉的数据行。</li></ul> |
| timeout | 否 | 加载作业的超时时间。有效值：`1` 到 `259200`。单位：秒。默认值：`600`。<br />**注意**除了 `timeout` 参数之外，您还可以使用[FE 参数](../../../administration/management/FE_configuration.md) `stream_load_default_timeout_second` 集中控制集群中所有 Stream Load 作业的超时时间。如果您指定了 `timeout` 参数，则以 `timeout` 参数指定的超时时间为准。如果您未指定 `timeout` 参数，则以 `stream_load_default_timeout_second` 参数指定的超时时间为准。|
| strict_mode | 否 | 指定是否启用[严格模式](../../../loading/load_concept/strict_mode.md)。有效值：`true` 和 `false`。默认值：`false`。值 `true` 表示启用严格模式，值 `false` 表示禁用严格模式。|
| timezone | 否 | 加载作业使用的时区。默认值：`Asia/Shanghai`。此参数的值会影响 strftime、alignment_timestamp 和 from_unixtime 等函数返回的结果。此参数指定的时区为会话级时区。更多信息，请参见[配置时区](../../../administration/management/timezone.md)。|
| load_mem_limit | 否 | 可分配给加载作业的最大内存量。单位：字节。默认情况下，加载作业的最大内存大小为 2 GB。此参数的值不能超过每个 BE 或 CN 可分配的最大内存量。|
| partial_update | 否 | 是否使用部分更新。有效值：`TRUE` 和 `FALSE`。默认值：`FALSE`，表示禁用此功能。|
| partial_update_mode | 否 | 指定部分更新的模式。有效值：`row` 和 `column`。<ul><li> 值 `row`（默认）表示行模式的部分更新，更适合列数较多、批量较小的实时更新场景。</li><li>值 `column` 表示列模式的部分更新，更适合列数较少、行数较多的批量更新场景。在此类场景中，启用列模式可提供更快的更新速度。例如，在一张有 100 列的表中，如果只更新 10 列（占总列数的 10%）的所有行，列模式的更新速度是行模式的 10 倍。</li></ul> |
| merge_condition | 否 | 指定用作条件的列名，以确定更新是否生效。只有当源数据记录在指定列中的值大于或等于目标数据记录时，从源记录到目标记录的更新才会生效。系统从 v2.5 版本开始支持条件更新。<br />**注意**<br />您指定的列不能是主键列。此外，只有使用主键表的表才支持条件更新。|

## 列映射

### 为 CSV 数据导入配置列映射

如果数据文件的列可以按顺序一一映射到表的列，则无需配置数据文件与表之间的列映射。

如果数据文件的列无法按顺序一一映射到表的列，则需要使用 `columns` 参数配置数据文件与表之间的列映射。这包括以下两种使用场景：

- **列数相同但列顺序不同。** **此外，数据文件中的数据在加载到匹配的表列之前无需通过函数计算。**

  在 `columns` 参数中，您需要按照数据文件列的排列顺序指定表列的名称。

  例如，表由三列组成，依次为 `col1`、`col2` 和 `col3`，数据文件也由三列组成，可依次映射到表列 `col3`、`col2` 和 `col1`。在这种情况下，您需要指定 `"columns: col3, col2, col1"`。

- **列数不同且列顺序不同。此外，数据文件中的数据在加载到匹配的表列之前需要通过函数计算。**

  在 `columns` 参数中，您需要按照数据文件列的排列顺序指定表列的名称，并指定要用于计算数据的函数。以下是两个示例：

  - 表由三列组成，依次为 `col1`、`col2` 和 `col3`。数据文件由四列组成，其中前三列可依次映射到表列 `col1`、`col2` 和 `col3`，第四列无法映射到任何表列。在这种情况下，您需要为数据文件的第四列临时指定一个名称，且该临时名称必须与任何表列名称不同。例如，您可以指定 `"columns: col1, col2, col3, temp"`，其中数据文件的第四列被临时命名为 `temp`。
  - 表由三列组成，依次为 `year`、`month` 和 `day`。数据文件只有一列，包含 `yyyy-mm-dd hh:mm:ss` 格式的日期和时间值。在这种情况下，您可以指定 `"columns: col, year = year(col), month=month(col), day=day(col)"`，其中 `col` 是数据文件列的临时名称，函数 `year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于从数据文件列 `col` 中提取数据并将其加载到对应的表列中。例如，`year = year(col)` 用于从数据文件列 `col` 中提取 `yyyy` 数据并将其加载到表列 `year` 中。

详细示例，请参见[配置列映射](#configure-column-mapping)。

### 为 JSON 数据导入配置列映射

如果 JSON 文档的键与表的列名称相同，您可以使用简单模式加载 JSON 格式的数据。在简单模式下，您无需指定 `jsonpaths` 参数。此模式要求 JSON 格式的数据必须是以花括号 `{}` 表示的对象，例如 `{"category": 1, "author": 2, "price": "3"}`。在此示例中，`category`、`author` 和 `price` 是键名，这些键可以按名称一一映射到表的列 `category`、`author` 和 `price`。

如果 JSON 文档的键名与表的列名不同，您可以使用匹配模式加载 JSON 格式的数据。在匹配模式下，您需要使用 `jsonpaths` 和 `COLUMNS` 参数来指定 JSON 文档与表之间的列映射关系：

- 在 `jsonpaths` 参数中，按照 JSON 文档中 JSON 键的排列顺序依次指定这些键。
- 在 `COLUMNS` 参数中，指定 JSON 键与表列之间的映射关系：
  - `COLUMNS` 参数中指定的列名按顺序与 JSON 键一一对应映射。
  - `COLUMNS` 参数中指定的列名按名称与表列一一对应映射。

有关使用匹配模式加载 JSON 格式数据的示例，请参见[使用匹配模式加载 JSON 数据](#load-json-data-using-matched-mode)。

## 返回值

导入作业完成后，系统以 JSON 格式返回作业结果。示例：

```JSON
{
    "TxnId": 1003,
    "Label": "label123",
    "Status": "Success",
    "Message": "OK",
    "NumberTotalRows": 1000000,
    "NumberLoadedRows": 999999,
    "NumberFilteredRows": 1,
    "NumberUnselectedRows": 0,
    "LoadBytes": 40888898,
    "LoadTimeMs": 2144,
    "BeginTxnTimeMs": 0,
    "StreamLoadPlanTimeMs": 1,
    "ReadDataTimeMs": 0,
    "WriteDataTimeMs": 11,
    "CommitAndPublishTimeMs": 16,
}
```

下表描述了返回的作业结果中的参数。

| 参数                   | 描述                                                         |
| ---------------------- | ------------------------------------------------------------ |
| TxnId                  | 导入作业的事务 ID。                                          |
| Label                  | 导入作业的标签。                                             |
| Status                 | 已加载数据的最终状态。<ul><li>`Success`：数据已成功加载，可以查询。</li><li>`Publish Timeout`：导入作业已成功提交，但数据暂时无法查询。您无需重试加载数据。</li><li>`Label Already Exists`：该导入作业的标签已被另一个导入作业使用。数据可能已成功加载或正在加载中。</li><li>`Fail`：数据加载失败。您可以重试导入作业。</li></ul> |
| Message                | 导入作业的状态。如果导入作业失败，则返回详细的失败原因。 |
| NumberTotalRows        | 读取的数据记录总数。                                         |
| NumberLoadedRows       | 成功加载的数据记录总数。仅当 `Status` 返回值为 `Success` 时，该参数有效。 |
| NumberFilteredRows     | 因数据质量不合格而被过滤掉的数据记录数。                     |
| NumberUnselectedRows   | 被 WHERE 子句过滤掉的数据记录数。                            |
| LoadBytes              | 已加载的数据量。单位：字节。                                 |
| LoadTimeMs             | 导入作业所用的时间。单位：毫秒。                             |
| BeginTxnTimeMs         | 为导入作业运行事务所用的时间。                               |
| StreamLoadPlanTimeMs   | 为导入作业生成执行计划所用的时间。                           |
| ReadDataTimeMs         | 为导入作业读取数据所用的时间。                               |
| WriteDataTimeMs        | 为导入作业写入数据所用的时间。                               |
| CommitAndPublishTimeMs | 为导入作业提交和发布数据所用的时间。                         |

如果导入作业失败，系统还会返回 `ErrorURL`。示例：

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL` 提供了一个 URL，您可以通过该 URL 获取已被过滤掉的不合格数据记录的详细信息。您可以在提交导入作业时，通过可选参数 `log_rejected_record_num` 指定可记录的不合格数据行的最大数量。

您可以运行 `curl "url"` 直接查看被过滤掉的不合格数据记录的详细信息，也可以运行 `wget "url"` 将这些数据记录的详细信息导出：

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

导出的数据记录详细信息将保存到本地文件，文件名类似于 `_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be`。您可以使用 `cat` 命令查看该文件。

然后，您可以调整导入作业的配置，并重新提交导入作业。

## 示例

<TableURLTip />

### 加载 CSV 数据

本节以 CSV 数据为例，介绍如何通过各种参数设置和组合来满足不同的加载需求。

#### 设置超时时间

您的数据库 `test_db` 中包含一张名为 `table1` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example1.csv` 也由三列组成，可按顺序映射到 `table1` 的 `col1`、`col2` 和 `col3`。

如果您希望在最多 100 秒内将 `example1.csv` 中的所有数据加载到 `table1`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    <table_url_prefix>/api/test_db/table1/_stream_load
```

#### 设置错误容忍度

您的数据库 `test_db` 中包含一张名为 `table2` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example2.csv` 也由三列组成，可按顺序映射到 `table2` 的 `col1`、`col2` 和 `col3`。

如果您希望以最大错误容忍度 `0.2` 将 `example2.csv` 中的所有数据加载到 `table2`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    <table_url_prefix>/api/test_db/table2/_stream_load
```

#### 配置列映射

您的数据库 `test_db` 中包含一张名为 `table3` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example3.csv` 也由三列组成，可按顺序映射到 `table3` 的 `col2`、`col1` 和 `col3`。

如果您希望将 `example3.csv` 中的所有数据加载到 `table3`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    <table_url_prefix>/api/test_db/table3/_stream_load
```

:::note
在上述示例中，`example3.csv` 的列无法按照这些列在 `table3` 中的排列顺序映射到 `table3` 的列。因此，您需要使用 `columns` 参数来配置 `example3.csv` 与 `table3` 之间的列映射关系。
:::

#### 设置过滤条件

您的数据库 `test_db` 中包含一张名为 `table4` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example4.csv` 也由三列组成，可以按顺序映射到 `col1`、`col2` 和 `col3`（属于 `table4`）。

如果您只想将 `example4.csv` 第一列中值等于 `20180601` 的数据记录加载到 `table4` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    <table_url_prefix>/api/test_db/table4/_stream_load
```

:::note
在上述示例中，`example4.csv` 和 `table4` 具有相同数量的列，可以按顺序映射，但您需要使用 WHERE 子句指定基于列的过滤条件。因此，您需要使用 `columns` 参数为 `example4.csv` 的列定义临时名称。
:::

#### 设置目标分区

您的数据库 `test_db` 包含一个名为 `table5` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example5.csv` 也由三列组成，可以按顺序映射到 `col1`、`col2` 和 `col3`（属于 `table5`）。

如果您想将 `example5.csv` 中的所有数据加载到 `table5` 的分区 `p1` 和 `p2` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    <table_url_prefix>/api/test_db/table5/_stream_load
```

#### 设置严格模式和时区

您的数据库 `test_db` 包含一个名为 `table6` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example6.csv` 也由三列组成，可以按顺序映射到 `col1`、`col2` 和 `col3`（属于 `table6`）。

如果您想使用严格模式和时区 `Africa/Abidjan` 将 `example6.csv` 中的所有数据加载到 `table6` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    <table_url_prefix>/api/test_db/table6/_stream_load
```

#### 将数据加载到包含 HLL 类型列的表中

您的数据库 `test_db` 包含一个名为 `table7` 的表。该表由两个 HLL 类型列组成，依次为 `col1` 和 `col2`。

您的数据文件 `example7.csv` 也由两列组成，其中第一列可以映射到 `table7` 的 `col1`，第二列无法映射到 `table7` 的任何列。`example7.csv` 第一列中的值可以在加载到 `table7` 的 `col1` 之前，通过函数转换为 HLL 类型数据。

如果您想将 `example7.csv` 中的数据加载到 `table7` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    <table_url_prefix>/api/test_db/table7/_stream_load
```

:::note
在上述示例中，`example7.csv` 的两列通过 `columns` 参数依次命名为 `temp1` 和 `temp2`。然后，使用函数对数据进行如下转换：

- 使用 `hll_hash` 函数将 `example7.csv` 的 `temp1` 中的值转换为 HLL 类型数据，并将 `example7.csv` 的 `temp1` 映射到 `table7` 的 `col1`。
- 使用 `hll_empty` 函数将指定的默认值填充到 `table7` 的 `col2` 中。
:::

有关函数 `hll_hash` 和 `hll_empty` 的用法，请参见 [hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 和 [hll_empty](../../sql-functions/scalar-functions/hll_empty.md)。

#### 将数据加载到包含 BITMAP 类型列的表中

您的数据库 `test_db` 包含一个名为 `table8` 的表。该表由两个 BITMAP 类型列组成，依次为 `col1` 和 `col2`。

您的数据文件 `example8.csv` 也由两列组成，其中第一列可以映射到 `table8` 的 `col1`，第二列无法映射到 `table8` 的任何列。`example8.csv` 第一列中的值可以在加载到 `table8` 的 `col1` 之前，通过函数进行转换。

如果您想将 `example8.csv` 中的数据加载到 `table8` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    <table_url_prefix>/api/test_db/table8/_stream_load
```

:::note
在上述示例中，`example8.csv` 的两列通过 `columns` 参数依次命名为 `temp1` 和 `temp2`。然后，使用函数对数据进行如下转换：

- 使用 `to_bitmap` 函数将 `example8.csv` 的 `temp1` 中的值转换为 BITMAP 类型数据，并将 `example8.csv` 的 `temp1` 映射到 `table8` 的 `col1`。
- 使用 `bitmap_empty` 函数将指定的默认值填充到 `table8` 的 `col2` 中。
:::

有关函数 `to_bitmap` 和 `bitmap_empty` 的用法，请参见 [to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) 和 [bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md)。

#### 设置 `skip_header`、`trim_space`、`enclose` 和 `escape`

您的数据库 `test_db` 包含一个名为 `table9` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example9.csv` 也由三列组成，按顺序映射到 `table13` 的 `col2`、`col1` 和 `col3`。

如果您想将 `example9.csv` 中的所有数据加载到 `table9` 中，同时跳过 `example9.csv` 的前五行、去除列分隔符前后的空格，并将 `enclose` 设置为 `\`、将 `escape` 设置为 `\`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    <table_url_prefix>/api/test_db/tbl9/_stream_load
```

#### 设置 `column_separator` 和 `row_delimiter`

您的数据库 `test_db` 中包含一张名为 `table10` 的表。该表由三列组成，依次为 `col1`、`col2` 和 `col3`。

您的数据文件 `example10.csv` 同样由三列组成，可依次映射到 `table10` 的 `col1`、`col2` 和 `col3`。数据行中的列之间以逗号（`,`）分隔，数据行之间以两个连续的不可打印字符 `\r\n` 分隔。

如果您想将 `example10.csv` 中的所有数据加载到 `table10`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label10" \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "row_delimiter:\\x0D0A" \
    -T example10.csv -XPUT \
    <table_url_prefix>/api/test_db/table10/_stream_load
```

### 加载 JSON 数据

本节介绍加载 JSON 数据时需要注意的参数设置。

您的数据库 `test_db` 中包含一张名为 `tbl1` 的表，其表结构如下：

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### 使用简单模式加载 JSON 数据

假设您的数据文件 `example1.json` 包含以下数据：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

要将 `example1.json` 中的所有数据加载到 `tbl1`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
在上述示例中，未指定参数 `columns` 和 `jsonpaths`。因此，`example1.json` 中的键将按名称映射到 `tbl1` 的列上。
:::

为提高吞吐量，Stream Load 支持一次性加载多条数据记录。示例：

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### 使用匹配模式加载 JSON 数据

系统执行以下步骤来匹配和处理 JSON 数据：

1. （可选）按照 `strip_outer_array` 参数设置的指示，去除最外层的数组结构。

   :::note
   仅当 JSON 数据的最外层为由一对方括号 `[]` 表示的数组结构时，才会执行此步骤。您需要将 `strip_outer_array` 设置为 `true`。
   :::

2. （可选）按照 `json_root` 参数设置的指示，匹配 JSON 数据的根元素。

   :::note
   仅当 JSON 数据存在根元素时，才会执行此步骤。您需要使用 `json_root` 参数指定根元素。
   :::

3. 按照 `jsonpaths` 参数设置的指示，提取指定的 JSON 数据。

##### 使用匹配模式加载 JSON 数据（不指定根元素）

假设您的数据文件 `example2.json` 包含以下数据：

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

要仅从 `example2.json` 中加载 `category`、`author` 和 `price`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label7" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" \
    -T example2.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
在上述示例中，JSON 数据的最外层为由一对方括号 `[]` 表示的数组结构。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将 `strip_outer_array` 设置为 `true` 以去除最外层的数组结构。键**标题**您不想加载的内容在加载过程中将被忽略。
:::

##### 使用匹配模式加载 JSON 数据（指定根元素）

假设您的数据文件 `example3.json` 包含以下数据：

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

要仅从 `example3.json` 中加载 `category`、`author` 和 `price`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "json_root: $.RECORDS" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" -H "label:label8" \
    -T example3.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
在上述示例中，JSON 数据的最外层为由一对方括号 `[]` 表示的数组结构。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将 `strip_outer_array` 设置为 `true` 以去除最外层的数组结构。您不想加载的键 `title` 和 `timestamp` 在加载过程中将被忽略。此外，`json_root` 参数用于指定 JSON 数据的根元素（即一个数组）。
:::

### 合并 Stream Load 请求

- 运行以下命令以同步模式启动启用了 Merge Commit 的 Stream Load 作业，并将合并窗口设置为 `5000` 毫秒，并行度设置为 `2`：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "column_separator:," \
      -H "columns: id, name, score" \
      -H "enable_merge_commit:true" \
      -H "merge_commit_interval_ms:5000" \
      -H "merge_commit_parallel:2" \
      -T example1.csv -XPUT \
      <table_url_prefix>/api/mydatabase/table1/_stream_load
  ```

- 运行以下命令以异步模式启动启用了 Merge Commit 的 Stream Load 作业，并将合并窗口设置为 `60000` 毫秒，并行度设置为 `2`：

  ```Bash
  curl --location-trusted -u <username>:<password> \
      -H "Expect:100-continue" \
      -H "column_separator:," \
      -H "columns: id, name, score" \
      -H "enable_merge_commit:true" \
      -H "merge_commit_async:true" \
      -H "merge_commit_interval_ms:60000" \
      -H "merge_commit_parallel:2" \
      -T example1.csv -XPUT \
      <table_url_prefix>/api/mydatabase/table1/_stream_load
  ```
