---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import Tip from '../../../_assets/commonMarkdown/quickstart-shared-nothing-tip.mdx';
import TableURL from '../../../_assets/commonMarkdown/stream_load_table_url.mdx';
import TableURLTip from '../../../_assets/commonMarkdown/stream_load_table_url_tip.mdx';

# STREAM LOAD

STREAM LOAD 允许您从本地文件系统或流式数据源加载数据。提交加载作业后，系统会同步运行该作业，并在作业完成后返回作业结果。您可以根据作业结果判断作业是否成功。有关 Stream Load 的应用场景、限制和支持的数据文件格式的信息，请参阅[通过 Stream Load 从本地文件系统加载数据](../../../loading/StreamLoad.md)。

<Tip />

从 v3.2.7 版本开始，Stream Load 支持在传输过程中压缩 JSON 数据，从而减少网络带宽开销。用户可以使用参数指定不同的压缩算法`compression` 和 `Content-Encoding`。支持的压缩算法包括 GZIP、BZIP2、LZ4_FRAME 和 ZSTD。有关更多信息，请参阅[data_desc](#data_desc)。

从 v3.4.0 版本开始，系统支持合并多个 Stream Load 请求。有关更多信息，请参阅[合并提交参数](#merge-commit-parameters)。

:::note

- 使用 Stream Load 将数据加载到原生表后，在该表上创建的物化视图的数据也会同步更新。
- 您只能以对原生表具有 INSERT 权限的用户身份将数据加载到原生表。如果您没有 INSERT 权限，请按照中提供的说明操作[GRANT](../account-management/GRANT.md)，将 INSERT 权限授予用于连接集群的用户。
:::

## 语法

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

本主题使用`curl`作为示例来描述如何使用 Stream Load 加载数据。除了`curl`之外，您还可以使用其他 HTTP 兼容的工具或语言执行 Stream Load。加载相关参数包含在 HTTP 请求头字段中。输入这些参数时，请注意以下几点：

- 您可以使用分块传输编码，如本主题所示。如果您不选择分块传输编码，则必须输入一个`Content-Length`头字段以指示要传输内容的长度，从而确保数据完整性。

  :::note
如果您使用`curl`执行 Stream Load，系统会自动添加一个`Content-Length`头字段，您无需手动输入。
:::

- 您必须添加一个`Expect`头字段并将其值指定为`100-continue`，例如`"Expect:100-continue"`。这有助于防止不必要的数据传输，并在您的作业请求被拒绝时减少资源开销。

请注意，在 StarRocks 中，某些字面量被 SQL 语言用作保留关键字。请勿在 SQL 语句中直接使用这些关键字。如果您想在 SQL 语句中使用此类关键字，请将其用一对反引号 (`) 括起来。请参阅[关键字](../keywords.md)。

## 参数

### 用户名和密码

指定用于连接集群的账户的用户名和密码。这是一个必需参数。如果您使用的账户未设置密码，则只需输入`<username>:`。

### XPUT

指定 HTTP 请求方法。这是一个必需参数。Stream Load 仅支持 PUT 方法。

### url

指定表的URL。语法：

<TableURL />

### data_desc

描述您要加载的数据文件。`data_desc`描述符可以包含数据文件的名称、格式、列分隔符、行分隔符、目标分区以及与表的列映射。语法：

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

中的参数`data_desc`描述符可分为三种类型：通用参数、CSV参数和JSON参数。

#### 通用参数

| 参数 | 必填 | 描述 |
| ---------- | -------- | ------------------------------------------------------------ |
| file_path | 是 | 数据文件的保存路径。您可以选择包含文件名的扩展名。 |
| format | 否 | 数据文件的格式。有效值：`CSV`和`JSON`。默认值：`CSV`。 |[临时分区](../../../table_design/data_distribution/Temporary_partition.md)，您希望将数据文件加载到其中。您可以指定多个临时分区，这些分区必须用逗号 (,) 分隔。|<br />如果数据文件中的字段可以按顺序映射到表中的列，则无需指定此参数。相反，您可以使用此参数来实现数据转换。例如，如果您加载一个CSV数据文件，并且该文件包含两个可以按顺序映射到表中两个列的列，即`id`和`city`，您可以指定`"columns: city,tmp_id, id = tmp_id * 100"`。有关更多信息，请参阅本主题中的“[列映射](#column-mapping)”部分。 |

#### CSV参数

| 参数 | 必填 | 描述 |
| ---------------- | -------- | ------------------------------------------------------------ |
| column_separator | 否 | 数据文件中用于分隔字段的字符。如果您未指定此参数，则此参数默认为`\t`，表示制表符。<br />请确保您使用此参数指定的列分隔符与数据文件中使用的列分隔符相同。<br />**注意**<br />- 对于CSV数据，您可以使用UTF-8字符串作为文本分隔符，例如逗号 (,)、制表符或管道符 (|)，其长度不超过50字节。<br />- 如果数据文件使用连续的不可打印字符（例如，`\r\n`）作为列分隔符，则必须将此参数设置为`\\x0D0A`。 |`\n`。<br />**注意**<br />如果数据文件使用连续的不可打印字符（例如，`\r\n`）作为行分隔符，则必须将此参数设置为`\\x0D0A`。 |`0`。 |<br />在某些CSV格式的数据文件中，开头的几行用于定义元数据，例如列名和列数据类型。通过设置`skip_header`参数，您可以使系统在数据加载期间跳过数据文件的前几行。例如，如果您将此参数设置为`1`，系统将在数据加载期间跳过数据文件的第一行。<br />数据文件开头的几行必须使用您在加载命令中指定的行分隔符进行分隔。 |`false`。 |<br />对于某些数据库，当您将数据导出为CSV格式的数据文件时，会在列分隔符中添加空格。这些空格根据其位置称为前导空格或尾随空格。通过设置`trim_space` 参数，您可以使系统在数据加载期间删除此类不必要的空格。<br />请注意，系统不会删除被一对`enclose`指定字符包裹的字段中的空格（包括前导空格和尾随空格）。例如，以下字段值使用管道符（<code class="language-text">|</code>）作为列分隔符，并使用双引号（`"`）作为`enclose`指定字符：<br /><code class="language-text">|"Love StarRocks"|</code> <br /><code class="language-text">|" Love StarRocks "|</code> <br /><code class="language-text">| "Love StarRocks" |</code> <br />如果您将`trim_space`设置为`true`，系统将按如下方式处理上述字段值：<br /><code class="language-text">|"Love StarRocks"|</code> <br /><code class="language-text">|" Love StarRocks "|</code> <br /><code class="language-text">|"Love StarRocks"|</code> |
| enclose | 否 | 指定根据[RFC4180](https://www.rfc-editor.org/rfc/rfc4180)规范在数据文件为 CSV 格式时用于包裹字段值的字符。类型：单字节字符。默认值：。最常见的字符是单引号（`NONE`）和双引号（`'`）。`"`所有使用<br />指定字符包裹的特殊字符（包括行分隔符和列分隔符）都被视为普通符号。系统可以做得比 RFC4180 更多，因为它允许您指定任何单字节字符作为`enclose`指定字符。`enclose`如果字段值包含<br />指定字符，您可以使用相同的字符来转义该`enclose`指定字符。例如，您将`enclose`设置为`enclose`，并且字段值为`"`。在这种情况下，您可以将字段值输入为`a "quoted" c`到数据文件中。 |
| escape | 否 | 指定用于转义各种特殊字符的字符，例如行分隔符、列分隔符、转义字符和`"a ""quoted"" c"`指定字符，这些字符随后被视为普通字符，并作为其所在字段值的一部分进行解析。类型：单字节字符。默认值：。最常见的字符是斜杠（`enclose`），在 SQL 语句中必须写成双斜杠（`NONE`）。`\`注意`\\`由<br />**指定的字符应用于每对**<br />指定字符的内部和外部。`escape`应用于每对的内部和外部`enclose`-指定的字符。<br />以下是两个示例：<ul><li>当您将`enclose`设置为`"`和`escape`设置为`\`时，系统会将`"say \"Hello world\""`解析为`say "Hello world"`。</li><li>假设列分隔符是逗号（`,`）。当您将`escape`设置为`\`时，系统会将`a, b\, c`解析为两个单独的字段值：`a`和`b, c`。</li></ul> |

:::note

- 对于 CSV 数据，您可以使用 UTF-8 字符串（例如逗号 (,)、制表符或竖线 (|)）作为文本分隔符，其长度不超过 50 字节。
- 空值通过使用`\N`表示。例如，一个数据文件包含三列，其中一条记录在第一列和第三列有数据，但在第二列没有数据。在这种情况下，您需要在第二列使用`\N`来表示空值。这意味着该记录必须编译为`a,\N,b`而不是`a,,b`。`a,,b`表示该记录的第二列包含一个空字符串。
- 格式选项，包括`skip_header`、`trim_space`、`enclose`和`escape`，在 v3.0 及更高版本中受支持。
:::

#### JSON 参数

| 参数 | 必填 | 描述 |
| --- | --- | --- |
| jsonpaths | 否 | 您要从 JSON 数据文件中加载的键的名称。仅当您使用匹配模式加载 JSON 数据时才需要指定此参数。此参数的值为 JSON 格式。请参阅[配置 JSON 数据加载的列映射](#configure-column-mapping-for-json-data-loading)。 |`true`| strip_outer_array | 否 | 指定是否剥离最外层数组结构。有效值：`false`和`false`。默认值：<br />。在实际业务场景中，JSON 数据可能具有由一对方括号`[]`表示的最外层数组结构。在这种情况下，我们建议您将此参数设置为`true`，以便系统移除最外层方括号`[]`并将每个内部数组作为单独的数据记录加载。如果您将此参数设置为`false`，系统将整个 JSON 数据文件解析为一个数组，并将该数组作为单个数据记录加载。<br />例如，JSON 数据是 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`。如果您将此参数设置为 `true`，则 `{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 将被解析为单独的数据记录，并加载到单独的表行中。 |
| json_root         | 否       | 您要从 JSON 数据文件中加载的 JSON 数据的根元素。仅当您使用匹配模式加载 JSON 数据时才需要指定此参数。此参数的值是有效的 JsonPath 字符串。默认情况下，此参数的值为空，表示将加载 JSON 数据文件的所有数据。有关更多信息，请参阅本主题的“[使用指定根元素的匹配模式加载 JSON 数据](#load-json-data-using-matched-mode-with-root-element-specified)”部分。 |
| ignore_json_size  | 否       | 指定是否检查 HTTP 请求中 JSON 主体的大小。<br />**注意**<br />默认情况下，HTTP 请求中 JSON 主体的大小不能超过 100 MB。如果 JSON 主体大小超过 100 MB，则会报告错误“此批次的大小超过 JSON 类型数据 [104857600] 的最大大小 [8617627793]。设置 ignore_json_size 以跳过检查，尽管这可能导致巨大的内存消耗。”为防止此错误，您可以在 HTTP 请求头中添加 `"ignore_json_size:true"`，以指示系统不检查 JSON 主体大小。 |
| compression, Content-Encoding | 否 | 数据传输过程中应用的编码算法。支持的算法包括 GZIP、BZIP2、LZ4_FRAME 和 ZSTD。示例： `curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。 |

加载 JSON 数据时，还要注意每个 JSON 对象的大小不能超过 4 GB。如果 JSON 数据文件中的单个 JSON 对象大小超过 4 GB，则会报告错误“此解析器不支持如此大的文档。”

### 合并提交参数

在指定时间窗口内为多个并发 Stream Load 请求启用合并提交，并将它们合并为一个事务。

:::warning

请注意，合并提交优化适用于在单个表上执行 **并发** Stream Load 任务的场景。如果并发度为一，则不建议使用。同时，在将 `merge_commit_async` 设置为 `false` 且将 `merge_commit_interval_ms` 设置为较大值之前请三思，因为它们可能导致加载性能下降。

:::

| **参数**            | **必填** | **描述**                                              |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| enable_merge_commit      | 否           | 是否为加载请求启用合并提交。有效值： `true` 和 `false`（默认）。 |
| merge_commit_async       | 否           | 服务器的返回模式。有效值：<ul><li>`true`：启用异步模式，服务器在接收数据后立即返回。此模式不保证加载成功。</li><li>`false`（默认）：启用同步模式，服务器仅在合并事务提交后返回，确保加载成功并可见。</li></ul> |
| merge_commit_interval_ms | 是          | 合并时间窗口的大小。单位：毫秒。合并提交尝试将在此窗口内收到的加载请求合并为一个事务。较大的窗口可提高合并效率，但会增加延迟。 |
| merge_commit_parallel    | 是          | 为每个合并窗口创建的加载计划的并行度。并行度可根据摄取负载进行调整。如果请求很多和/或要加载的数据量很大，请增加此值。并行度受限于 BE 节点的数量，计算方式为 `min(merge_commit_parallel, number of BE nodes)`。 |

:::note

- 合并提交仅支持将 **同构** 加载请求合并到单个数据库和表中。“同构”表示 Stream Load 参数相同，包括：通用参数、JSON 格式参数、CSV 格式参数、`opt_properties`和 Merge Commit 参数。
- 对于加载 CSV 格式的数据，您必须确保每行都以换行符结尾。`skip_header` 不支持。
- 服务器会自动为事务生成标签。如果指定了标签，则会忽略。
- Merge Commit 将多个加载请求合并到一个事务中。如果其中一个请求包含数据质量问题，则事务中的所有请求都将失败。

:::

### opt_properties

指定一些可选参数，这些参数应用于整个加载作业。语法：

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

| 参数 | 必填 | 描述 |
| --- | --- | --- |
| label | 否 | 加载作业的标签。如果您不指定此参数，系统会自动为加载作业生成一个标签。<br />系统不允许您使用一个标签多次加载数据批次。因此，系统会阻止重复加载相同的数据。有关标签命名约定，请参阅 [系统限制](../../System_limit.md)。<br />默认情况下，系统会保留最近三天内成功完成的加载作业的标签。您可以使用 [FE 参数](../../../administration/management/FE_configuration.md) `label_keep_max_second` 来更改标签保留期。 |
| where | 否 | 系统根据其过滤预处理数据的条件。系统仅加载满足 WHERE 子句中指定的过滤条件的预处理数据。 |
| max_filter_ratio | 否 | 加载作业的最大错误容忍度。错误容忍度是指在加载作业请求的所有数据记录中，由于数据质量不足而被过滤掉的数据记录的最大百分比。有效值：`0` 到 `1`。默认值：`0`。<br />我们建议您保留默认值 `0`。这样，如果检测到不合格的数据记录，加载作业将失败，从而确保数据的正确性。<br />如果您想忽略不合格的数据记录，可以将此参数设置为大于 `0` 的值。这样，即使数据文件包含不合格的数据记录，加载作业也可以成功。<br />**注意**<br />不合格的数据记录不包括被 WHERE 子句过滤掉的数据记录。 |
| log_rejected_record_num | 否 | 指定可以记录的不合格数据行的最大数量。此参数从 v3.1 开始支持。有效值：`0`、`-1`，以及任何非零正整数。默认值：`0`。<ul><li>值 `0` 表示被过滤掉的数据行将不会被记录。</li><li>值 `-1` 表示所有被过滤掉的数据行都将被记录。</li><li>一个非零正整数，例如 `n` 表示每个 BE 或 CN 上最多可以记录 `n` 条被过滤掉的数据行。</li></ul> |
| timeout | 否 | 加载作业的超时时间。有效值：`1` 到 `259200`。单位：秒。默认值：`600`。<br />**注意**除了`timeout`参数外，您还可以使用[FE 参数](../../../administration/management/FE_configuration.md) `stream_load_default_timeout_second`来集中控制集群中所有 Stream Load 作业的超时时间。如果您指定了`timeout`参数，则以`timeout`参数指定的超时时间为准。如果您未指定`timeout`参数，则以`stream_load_default_timeout_second`参数指定的超时时间为准。|
| strict_mode      | 否       | 指定是否启用[严格模式](../../../loading/load_concept/strict_mode.md)。有效值：`true`和`false`。默认值：`false`。值`true`表示启用严格模式，值`false`表示禁用严格模式。|
| timezone         | 否       | 加载作业使用时区。默认值：`Asia/Shanghai`。此参数的值会影响 strftime、alignment_timestamp 和 from_unixtime 等函数返回的结果。此参数指定的时区是会话级时区。有关更多信息，请参阅[配置时区](../../../administration/management/timezone.md)。|
| load_mem_limit   | 否       | 可为加载作业分配的最大内存量。单位：字节。默认情况下，加载作业的最大内存大小为 2 GB。此参数的值不能超过可为每个 BE 或 CN 分配的最大内存量。|
| partial_update | 否 | 是否使用部分更新。有效值：`TRUE`和`FALSE`。默认值：`FALSE`，表示禁用此功能。|
| partial_update_mode | 否 | 指定部分更新的模式。有效值：`row`和`column`。<ul><li>值`row`（默认）表示行模式下的部分更新，更适用于列多、小批量实时更新的场景。</li><li>值`column`表示列模式下的部分更新，更适用于列少、多行批量更新的场景。在此类场景下，启用列模式可提供更快的更新速度。例如，在一个有 100 列的表中，如果所有行只更新 10 列（总数的 10%），则列模式的更新速度会快 10 倍。</li></ul>|
| merge_condition  | 否       | 指定要用作条件以确定更新是否生效的列的名称。只有当源数据记录在指定列中的值大于或等于目标数据记录时，从源记录到目标记录的更新才会生效。系统自 v2.5 起支持条件更新。<br />**注意**<br />您指定的列不能是主键列。此外，只有使用主键表的表才支持条件更新。|

## 列映射

### 配置 CSV 数据加载的列映射

如果数据文件的列可以按顺序与表的列一一对应，则无需配置数据文件与表之间的列映射。

如果数据文件的列不能按顺序与表的列一一对应，则需要使用`columns`参数配置数据文件与表之间的列映射。这包括以下两种使用场景：

- **列数相同但列顺序不同。** **此外，数据文件中的数据在加载到匹配的表列之前无需通过函数计算。**

  在`columns`参数中，您需要按照数据文件列的排列顺序指定表列的名称。

  例如，表包含三列，分别是`col1`，`col2`，以及`col3`的顺序，并且数据文件也包含三列，这些列可以按顺序映射到表列`col3`，`col2`，以及`col1`。在这种情况下，您需要指定`"columns: col3, col2, col1"`。

- **列数不同，列顺序也不同。此外，数据文件中的数据在加载到匹配的表列之前，需要通过函数进行计算。**

  在`columns`参数中，您需要按照数据文件列的排列顺序指定表列的名称，并指定用于计算数据的函数。以下是两个示例：

  - 该表包含三列，分别是`col1`，`col2`，以及`col3`。数据文件包含四列，其中前三列可以按顺序映射到表列`col1`，`col2`，以及`col3`，第四列不能映射到任何表列。在这种情况下，您需要为数据文件的第四列临时指定一个名称，并且该临时名称必须与任何表列名称不同。例如，您可以指定`"columns: col1, col2, col3, temp"`，其中数据文件的第四列临时命名为`temp`。
  - 该表包含三列，分别是`year`，`month`，以及`day`。数据文件只包含一列，该列以`yyyy-mm-dd hh:mm:ss`格式容纳日期和时间值。在这种情况下，您可以指定`"columns: col, year = year(col), month=month(col), day=day(col)"`，其中`col`是数据文件列的临时名称，函数`year = year(col)`，`month=month(col)`，以及`day=day(col)`用于从数据文件列`col`中提取数据并将其加载到映射的表列中。例如，`year = year(col)`用于提取`yyyy`数据从数据文件列`col`并加载数据到表列`year`。

有关详细示例，请参阅[配置列映射](#configure-column-mapping)。

### 配置JSON数据加载的列映射

如果JSON文档的键与表的列名相同，您可以使用简单模式加载JSON格式的数据。在简单模式下，您无需指定`jsonpaths`参数。此模式要求JSON格式的数据必须是一个对象，由花括号表示`{}`，例如`{"category": 1, "author": 2, "price": "3"}`。在此示例中，`category`、`author`和`price`是键名，这些键可以按名称与表的列`category`、`author`和`price`一对一映射。

如果 JSON 文档的键名与表的列名不同，您可以使用匹配模式加载 JSON 格式的数据。在匹配模式下，您需要使用`jsonpaths`和`COLUMNS`参数来指定 JSON 文档和表之间的列映射：

- 在`jsonpaths`参数中，按照 JSON 文档中键的排列顺序指定 JSON 键。
- 在`COLUMNS`参数中，指定 JSON 键和表列之间的映射：
  - 在`COLUMNS`参数中指定的列名按顺序与 JSON 键一对一映射。
  - 在`COLUMNS`参数中指定的列名按名称与表列一对一映射。

有关使用匹配模式加载 JSON 格式数据的示例，请参见[使用匹配模式加载 JSON 数据](#load-json-data-using-matched-mode)。

## 返回值

加载作业完成后，系统以 JSON 格式返回作业结果。例如：

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

| 参数 | 描述 |
| ---------------------- | ------------------------------------------------------------ |
| TxnId | 加载作业的事务 ID。 |
| Label | 加载作业的标签。 |
| Status | 加载数据的最终状态。<ul><li>`Success`：数据已成功加载并可查询。</li><li>`Publish Timeout`：加载作业已成功提交，但数据仍无法查询。您无需重试加载数据。</li><li>`Label Already Exists`：加载作业的标签已被其他加载作业使用。数据可能已成功加载或正在加载。</li><li>`Fail`：数据加载失败。您可以重试加载作业。</li></ul> |
| Message | 加载作业的状态。如果加载作业失败，则返回详细的失败原因。 |
| NumberTotalRows | 读取的数据记录总数。 |
| NumberLoadedRows | 成功加载的数据记录总数。此参数仅在`Status`为`Success`时有效。 |

如果加载作业失败，系统还会返回`ErrorURL`。例如：

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL`提供一个 URL，您可以从中获取有关已过滤掉的不合格数据记录的详细信息。您可以使用可选参数`log_rejected_record_num`来指定可记录的不合格数据行的最大数量，该参数在您提交加载作业时设置。

您可以运行`curl "url"`直接查看已过滤掉的不合格数据记录的详细信息。您还可以运行`wget "url"`导出这些数据记录的详细信息：

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

导出的数据记录详情将保存到本地文件中，文件名类似于`_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be`。您可以使用`cat`命令查看文件。

然后，您可以调整加载作业的配置，并再次提交加载作业。

## 示例

<TableURLTip />

### 加载 CSV 数据

本节以 CSV 数据为例，介绍如何通过各种参数设置和组合来满足您多样化的加载需求。

#### 设置超时时间

您的数据库`test_db`包含一个名为`table1`的表。该表包含三列，分别是`col1`、`col2`和`col3`依次排列。

您的数据文件`example1.csv`也包含三列，可以依次映射到`col1`、`col2`和`col3`的`table1`中。

如果您想将所有数据从`example1.csv`加载到`table1`中，且最长不超过 100 秒，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    <table_url_prefix>/api/test_db/table1/_stream_load
```

#### 设置错误容忍度

您的数据库`test_db`包含一个名为`table2`的表。该表包含三列，分别是`col1`、`col2`和`col3`依次排列。

您的数据文件`example2.csv`也包含三列，可以依次映射到`col1`、`col2`和`col3`的`table2`中。

如果您想将所有数据从`example2.csv`加载到`table2`中，且最大错误容忍度为`0.2`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    <table_url_prefix>/api/test_db/table2/_stream_load
```

#### 配置列映射

您的数据库`test_db`包含一个名为`table3`的表。该表包含三列，分别为`col1`、`col2`和`col3`，按此顺序排列。

您的数据文件`example3.csv`也包含三列，可以按顺序映射到`col2`、`col1`和`col3`的`table3`中。

如果您想将所有数据从`example3.csv`加载到`table3`中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    <table_url_prefix>/api/test_db/table3/_stream_load
```

:::note
在上述示例中，`example3.csv`的列无法按其在`table3`中排列的相同顺序映射到`table3`的列。因此，您需要使用`columns`参数来配置`example3.csv`和`table3`之间的列映射。
:::

#### 设置过滤条件

您的数据库`test_db`包含一个名为`table4`的表。该表包含三列，分别为`col1`、`col2`和`col3`，按此顺序排列。

您的数据文件`example4.csv`也包含三列，可以按顺序映射到`col1`、`col2`和`col3`的`table4`中。

如果您只想将`example4.csv`的第一列中值等于`20180601`的数据记录加载到`table4`中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2, col3"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    <table_url_prefix>/api/test_db/table4/_stream_load
```

:::note
在前面的示例中，`example4.csv` 和 `table4` 具有相同数量的列，可以按顺序映射，但您需要使用 WHERE 子句来指定基于列的筛选条件。因此，您需要使用 `columns` 参数为 的列定义临时名称。`example4.csv`.
:::

#### 设置目标分区

您的数据库 `test_db` 包含一个名为 `table5` 的表。该表包含三列，分别是 `col1`、`col2` 和 `col3`，按顺序排列。

您的数据文件 `example5.csv` 也包含三列，可以按顺序映射到 `col1`、`col2` 和 `col3` 的 `table5`。

如果您想将所有数据从 `example5.csv` 加载到分区 `p1` 和 `p2` 的 `table5` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    <table_url_prefix>/api/test_db/table5/_stream_load
```

#### 设置严格模式和时区

您的数据库 `test_db` 包含一个名为 `table6` 的表。该表包含三列，分别是 `col1`、`col2` 和 `col3`，按顺序排列。

您的数据文件 `example6.csv` 也包含三列，可以按顺序映射到 `col1`、`col2` 和 `col3` 的 `table6`。

如果您想将所有数据从 `example6.csv` 加载到 `table6` 中，并使用严格模式和时区 `Africa/Abidjan`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    <table_url_prefix>/api/test_db/table6/_stream_load
```

#### 将数据加载到包含HLL类型列的表中

您的数据库`test_db`包含一个名为`table7`的表。该表包含两个HLL类型列，它们是`col1`和`col2`（按顺序）。

您的数据文件`example7.csv`也包含两列，其中第一列可以映射到`col1`的`table7`，第二列不能映射到`table7`的任何列。在`example7.csv`的第一列中的值可以在加载到`col1`的`table7`之前，通过使用函数转换为HLL类型数据。

如果您想将数据从`example7.csv`加载到`table7`中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    <table_url_prefix>/api/test_db/table7/_stream_load
```

:::note
在前面的示例中，`example7.csv`的两列被命名为`temp1`和`temp2`（按顺序），通过使用`columns`参数。然后，使用函数转换数据，如下所示：

- 函数用于将`hll_hash`中的值转换为HLL类型数据，并将`temp1`的`example7.csv`映射到`temp1`的`example7.csv`的`col1`。`table7`。
- 函数用于将指定的默认值填充到`hll_empty`的`col2`中。
:::`table7`。

有关函数`hll_hash`和`hll_empty`的用法，请参阅[hll_hash](../../sql-functions/scalar-functions/hll_hash.md)和[hll_empty](../../sql-functions/scalar-functions/hll_empty.md)。

#### 将数据加载到包含 BITMAP 类型列的表中

您的数据库 `test_db` 包含一个名为 `table8` 的表。该表包含两个 BITMAP 类型的列，分别是 `col1` 和 `col2`，按顺序排列。

您的数据文件 `example8.csv` 也包含两列，其中第一列可以映射到 `col1` 的 `table8`，第二列不能映射到 `table8` 的任何列。 `example8.csv` 的第一列中的值可以在加载到 `col1` 的 `table8` 之前使用函数进行转换。

如果要将数据从 `example8.csv` 加载到 `table8` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    <table_url_prefix>/api/test_db/table8/_stream_load
```

:::note
在前面的示例中，`example8.csv` 的两列被命名为 `temp1` 和 `temp2`，按顺序使用 `columns` 参数。然后，使用函数按如下方式转换数据：

-  函数用于将 `to_bitmap` 中的值转换为 BITMAP 类型数据，并将 `temp1` 的 `example8.csv` 映射到 `temp1` 的 `example8.csv``col1` 的 `table8`。
-  函数用于将指定默认值填充到 `bitmap_empty` 的 `col2` 中。
:::`table8`。

有关函数 `to_bitmap` 和 `bitmap_empty` 的用法，请参见 [to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) 和 [位图为空](../../sql-functions/bitmap-functions/bitmap_empty.md)。

#### 设置 `skip_header`，`trim_space`，`enclose`，和 `escape`

您的数据库 `test_db` 包含一个名为 `table9` 的表。该表包含三列，分别是 `col1`，`col2`，和 `col3`，按顺序排列。

您的数据文件 `example9.csv` 也包含三列，它们按顺序映射到 `col2`，`col1`，和 `col3` 的 `table13`。

如果您想将所有数据从 `example9.csv` 加载到 `table9` 中，并跳过 `example9.csv` 的前五行，删除列分隔符前后空格，并将 `enclose` 设置为 `\`，将 `escape` 设置为 `\`，请运行以下命令：

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

您的数据库 `test_db` 包含一个名为 `table10` 的表。该表包含三列，分别是 `col1`，`col2`，和 `col3`，按顺序排列。

您的数据文件 `example10.csv` 也包含三列，它们可以按顺序映射到 `col1`，`col2`，和 `col3` 的 `table10`。数据行中的列由逗号分隔（`,`)，数据行由两个连续的不可打印字符分隔`\r\n`。

如果您想将所有数据从`example10.csv`加载到`table10`中，运行以下命令：

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

您的数据库`test_db`包含一个名为`tbl1`的表，其 schema 如下：

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### 使用简单模式加载 JSON 数据

假设您的数据文件`example1.json`包含以下数据：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

要将所有数据从`example1.json`加载到`tbl1`中，运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
在前面的示例中，参数`columns`和`jsonpaths`未指定。因此，`example1.json`中的键按名称映射到`tbl1`的列。
:::

为提高吞吐量，Stream Load 支持一次性加载多条数据记录。例如：

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### 使用匹配模式加载 JSON 数据

系统执行以下步骤来匹配和处理 JSON 数据：

1. (可选) 根据`strip_outer_array`参数设置剥离最外层数组结构。

   :::note
此步骤仅在 JSON 数据的最外层是数组结构（由一对中括号`[]`表示）时执行。您需要将`strip_outer_array`设置为`true`。
:::

2. (可选) 根据`json_root`参数设置匹配 JSON 数据的根元素。

   :::note
此步骤仅在 JSON 数据具有根元素时执行。您需要使用`json_root`参数指定根元素。
:::

3. 根据`jsonpaths`参数设置提取指定的 JSON 数据。

##### 使用匹配模式加载 JSON 数据（未指定根元素）

假设您的数据文件`example2.json`包含以下数据：

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

要仅加载`category`、`author`和`price`从`example2.json`，运行以下命令：

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
在前面的示例中，JSON 数据的最外层是一个数组结构，由一对方括号表示`[]`。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将`strip_outer_array`设置为`true`以剥离最外层的数组结构。键**title**（您不想加载）在加载时会被忽略。
:::

##### 使用匹配模式加载指定根元素的 JSON 数据

假设您的数据文件`example3.json`包含以下数据：

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

要仅加载`category`、`author`和`price`从`example3.json`，运行以下命令：

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
在前面的示例中，JSON 数据的最外层是一个数组结构，由一对方括号表示`[]`。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将`strip_outer_array`设置为`true`以剥离最外层的数组结构。键`title`和`timestamp`（您不想加载）在加载时会被忽略。此外，`json_root`参数用于指定 JSON 数据的根元素（即一个数组）。
:::

### 合并 Stream Load 请求

- 运行以下命令以同步模式启动一个启用合并提交的 Stream Load 作业，并将合并窗口设置为`5000`毫秒，并将并行度设置为`2`：

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

- 运行以下命令以异步模式启动一个启用合并提交的 Stream Load 作业，并将合并窗口设置为`60000`毫秒，并将并行度设置为`2`：

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
