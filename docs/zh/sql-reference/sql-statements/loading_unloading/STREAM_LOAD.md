---
displayed_sidebar: docs
toc_max_heading_level: 4
---

import TableURL from '../../../_assets/commonMarkdown/stream_load_table_url.mdx';
import TableURLTip from '../../../_assets/commonMarkdown/stream_load_table_url_tip.mdx';

# STREAM LOAD

STREAM LOAD 允许您从本地文件系统或流式数据源导入数据。提交导入作业后，系统会同步运行该作业，并在作业完成后返回作业结果。您可以根据作业结果判断作业是否成功。有关 Stream Load 的应用场景、限制和支持的数据文件格式的信息，请参阅[通过 Stream Load 从本地文件系统导入数据](../../../loading/StreamLoad.md)。

从 v3.2.7 版本开始，Stream Load 支持在传输过程中压缩 JSON 数据，从而减少网络带宽开销。用户可以使用参数指定不同的压缩算法 `compression` 和 `Content-Encoding`。支持的压缩算法包括 GZIP、BZIP2、LZ4_FRAME 和 ZSTD。有关更多信息，请参阅[data_desc](#data_desc)。

从 v3.4.0 版本开始，系统支持合并多个 Stream Load 请求。有关更多信息，请参阅[Merge Commit 参数](#merge-commit-参数)。

:::note
- 使用 Stream Load 将数据导入到内表后，在该表上创建的物化视图的数据也会同步更新。
- 您只能以对当前内表具有 INSERT 权限的用户身份将数据导入到内表。如果您没有 INSERT 权限，请按照中提供的说明操作[GRANT](../account-management/GRANT.md)，将 INSERT 权限授予连接集群的用户。
:::

## 语法

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

本文使用 `curl` 作为示例来描述如何使用 Stream Load 导入数据。除了 `curl` 之外，您还可以使用其他 HTTP 兼容的工具或语言执行 Stream Load。导入相关参数包含在 HTTP 请求Header中。输入这些参数时，请注意以下几点：

- 您可以使用分块传输编码，如本主题所示。如果您不选择分块传输编码，则必须输入一个 `Content-Length` Header 以指示要传输内容的长度，从而确保数据完整性。

  :::note
  如果您使用`curl`执行 Stream Load，系统会自动添加一个 `Content-Length` Header，您无需手动输入。
  :::

- 您必须添加一个 `Expect` Header 并将其值指定为 `100-continue`，例如 `"Expect:100-continue"`。这有助于防止不必要的数据传输，并在您的作业请求被拒绝时减少资源开销。

请注意，在 StarRocks 中，某些词被 SQL 语言用作保留关键字。请勿在 SQL 语句中直接使用这些关键字。如果您想在 SQL 语句中使用此类关键字，请将其用一对反引号 (`) 括起来。请参阅[关键字](../keywords.md)。

## 参数

### 用户名和密码

指定用于连接集群的账户的用户名和密码。这是一个必需参数。如果您使用的账户未设置密码，则只需输入 `<username>:`。

### XPUT

指定 HTTP 请求方法。这是一个必需参数。Stream Load 仅支持 PUT 方法。

### url

指定表的 URL。语法：

<TableURL />

### data_desc

描述您要导入的数据文件。`data_desc` 描述符可以包含数据文件的名称、格式、列分隔符、行分隔符、目标分区以及与表的列映射。语法：

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

其中的参数 `data_desc` 描述符可分为三种类型：通用参数、CSV 参数和 JSON 参数。

#### 通用参数

| **参数名称** | **是否必选** | **参数说明**                                                 |
| ------------ | ------------ | ------------------------------------------------------------ |
| file_path    | 是           | 指定源数据文件的保存路径。文件名里可选包含或者不包含扩展名。 |
| format       | 否           | 指定待导入数据的格式。取值包括 `CSV` 和 `JSON`。默认值：`CSV`。 |
| partitions   | 否           | 指定要把数据导入哪些分区。如果不指定该参数，则默认导入到表所在的所有分区中。 |
| temporary_partitions | 否   | 指定要把数据导入哪些[临时分区](../../../table_design/data_distribution/Temporary_partition.md)。|
| columns      | 否           | 指定源数据文件和表之间的列对应关系。如果源数据文件中的列与表中的列按顺序一一对应，则不需要指定 `columns` 参数。您可以通过 `columns` 参数实现数据转换。例如，要导入一个 CSV 格式的数据文件，文件中有两列，分别可以对应到目标表的 `id` 和 `city` 两列。如果要实现把数据文件中第一列的数据乘以 100 以后再落入表的转换，可以指定 `"columns: city,tmp_id, id = tmp_id * 100"`。具体请参见本文“[列映射](#列映射)”章节。 |

#### CSV 参数

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| column_separator | 否           | 用于指定源数据文件中的列分隔符。如果不指定该参数，则默认为 `\t`，即 Tab。必须确保这里指定的列分隔符与源数据文件中的列分隔符一致。<br />**说明**<br />- 系统支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (\|)。- 若数据文件使用连续的不可打印字符（例如 `\r\n`）作为列分隔符，则必须将此参数设置为 `\\x0D0A`。 |
| row_delimiter    | 否           | 用于指定源数据文件中的行分隔符。如果不指定该参数，则默认为 `\n`。<br />**注意**<br />如果数据文件使用连续的不可打印字符（例如 `\r\n`）作为行分隔符，则必须将此参数设置为 `\\x0D0A`。 |
| skip_header      | 否           | 用于指定跳过 CSV 文件最开头的几行数据。取值类型：INTEGER。默认值：`0`。<br />在某些 CSV 文件里，最开头的几行数据会用来定义列名、列类型等元数据信息。通过设置该参数，可以使系统在导入数据时忽略 CSV 文件的前面几行。例如，如果设置该参数为 `1`，则系统会在导入数据时忽略 CSV 文件的第一行。<br />这里的行所使用的分隔符须与您在导入命令中所设定的行分隔符一致。 |
| trim_space       | 否           | 用于指定是否去除 CSV 文件中列分隔符前后的空格。取值类型：BOOLEAN。默认值：`false`。<br />有些数据库在导出数据为 CSV 文件时，会在列分隔符的前后添加一些空格。根据位置的不同，这些空格可以称为“前导空格”或者“尾随空格”。通过设置该参数，可以使系统在导入数据时删除这些不必要的空格。<br />需要注意的是，系统不会去除被 `enclose` 指定字符括起来的字段内的空格（包括字段的前导空格和尾随空格）。例如，列分隔符是竖线 (<code class="language-text">&#124;</code>)，`enclose` 指定的字符是双引号 (`"`)：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />如果设置 `trim_space` 为 `true`，则系统处理后的结果数据如下：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose          | 否           | 根据 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180)，用于指定把 CSV 文件中的字段括起来的字符。取值类型：单字节字符。默认值：`NONE`。最常用 `enclose` 字符为单引号 (`'`) 或双引号 (`"`)。<br />被 `enclose` 指定字符括起来的字段内的所有特殊字符（包括行分隔符、列分隔符等）均看做是普通符号。比 RFC4180 标准更进一步的是，系统提供的 `enclose` 属性支持设置任意单个字节的字符。<br />如果一个字段内包含了 `enclose` 指定字符，则可以使用同样的字符对 `enclose` 指定字符进行转义。例如，在设置了`enclose` 为双引号 (`"`) 时，字段值 `a "quoted" c` 在 CSV 文件中应该写作 `"a ""quoted"" c"`。 |
| escape           | 否           | 指定用于转义的字符。用来转义各种特殊字符，比如行分隔符、列分隔符、转义符、`enclose` 指定字符等，使系统把这些特殊字符当做普通字符而解析成字段值的一部分。取值类型：单字节字符。默认值：`NONE`。最常用的 `escape` 字符为斜杠 (`\`)，在 SQL 语句中应该写作双斜杠 (`\\`)。<br />**说明**<br />`escape` 指定字符同时作用于 `enclose` 指定字符的内部和外部。<br />以下为两个示例：<ul><li>当设置 `enclose` 为双引号 (`"`) 、`escape` 为斜杠 (`\`) 时，系统会把 `"say \"Hello world\""` 解析成一个字段值 `say "Hello world"`。</li><li>假设列分隔符为逗号 (`,`) ，当设置 `escape` 为斜杠 (`\`) ，系统会把 `a, b\, c` 解析成 `a` 和 `b, c` 两个字段值。</li></ul> |

:::note

- 对于 CSV 数据，您可以使用 UTF-8 字符串（例如逗号 (,)、制表符或竖线 (|)）作为文本分隔符，其长度不超过 50 字节。
- 空值通过使用`\N`表示。例如，一个数据文件包含三列，其中一条记录在第一列和第三列有数据，但在第二列没有数据。在这种情况下，您需要在第二列使用`\N`来表示空值。这意味着该记录必须编译为`a,\N,b`而不是`a,,b`。`a,,b`表示该记录的第二列包含一个空字符串。
- 格式选项，包括`skip_header`、`trim_space`、`enclose`和`escape`，在 v3.0 及更高版本中受支持。
:::

#### JSON 参数

| **参数名称**      | **是否必选** | **参数说明**                                                 |
| ----------------- | ------------ | ------------------------------------------------------------ |
| jsonpaths         | 否           | 用于指定待导入的字段的名称。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为 JSON 格式。参见[导入 JSON 数据时配置列映射关系](#导入-json-数据时配置列映射关系)。    |
| strip_outer_array | 否           | 用于指定是否裁剪最外层的数组结构。取值范围：`true` 和 `false`。默认值：`false`。真实业务场景中，待导入的 JSON 数据可能在最外层有一对表示数组结构的中括号 `[]`。这种情况下，一般建议您指定该参数取值为 `true`，这样系统会剪裁掉外层的中括号 `[]`，并把中括号 `[]` 里的每个内层数组都作为一行单独的数据导入。如果您指定该参数取值为 `false`，则系统会把整个 JSON 数据文件解析成一个数组，并作为一行数据导入。例如，待导入的 JSON 数据为 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`，如果指定该参数取值为 `true`，则系统会把 `{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 解析成两行数据，并导入到目标表中对应的数据行。 |
| json_root         | 否           | 用于指定待导入 JSON 数据的根元素。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为合法的 JsonPath 字符串。默认值为空，表示会导入整个 JSON 数据文件的数据。具体请参见本文提供的示例“[导入数据并指定 JSON 根节点](#指定-json-根节点使用匹配模式导入数据)”。 |
| ignore_json_size | 否   | 用于指定是否检查 HTTP 请求中 JSON Body 的大小。<br />**说明**<br />HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。 |
| compression, Content-Encoding | 否 | 指定在 STREAM LOAD 数据传输过程中使用哪种压缩算法，支持 GZIP、BZIP2、LZ4_FRAME、ZSTD 算法。示例：`curl --location-trusted -u root:  -v '<table_url>' \-X PUT  -H "expect:100-continue" \-H 'format: json' -H 'compression: lz4_frame'   -T ./b.json.lz4`。 |

导入 JSON 数据时，还要注意每个 JSON 对象的大小不能超过 4 GB。如果 JSON 数据文件中的单个 JSON 对象大小超过 4 GB，则会报告错误“此解析器不支持如此大的文档。”

### Merge Commit 参数

为在指定时间窗口内多个并发的 Stream Load 请求启用 Merge Commit，并将它们合并为一个事务。

:::warning

请注意，Merge Commit 优化适用于在单个表上执行**并发** Stream Load 任务的场景。如果并发度为一，则不建议使用。同时，在将 `merge_commit_async` 设置为 `false` 且将 `merge_commit_interval_ms` 设置为较大值之前请三思，因为它们可能导致导入性能下降。

:::

| **参数名称**              | **是否必选** | **参数说明**                                                 |
| ------------------------ | ------------ | ------------------------------------------------------------ |
| enable_merge_commit      | 否           | 是否为加载请求启用Merge Commit。有效值： `true` 和 `false`（默认）。 |
| merge_commit_async       | 否           | 服务器的返回模式。有效值：<ul><li>`true`：启用异步模式，服务器在接收数据后立即返回。此模式不保证加载成功。</li><li>`false`（默认）：启用同步模式，服务器仅在合并事务提交后返回，确保加载成功并可见。</li></ul> |
| merge_commit_interval_ms | 是          | 合并时间窗口的大小。单位：毫秒。Merge Commit尝试将在此窗口内收到的加载请求合并为一个事务。较大的窗口可提高合并效率，但会增加延迟。 |
| merge_commit_parallel    | 是          | 为每个合并窗口创建的加载计划的并行度。并行度可根据摄取负载进行调整。如果请求很多和/或要加载的数据量很大，请增加此值。并行度受限于 BE 节点的数量，计算方式为 `min(merge_commit_parallel, number of BE nodes)`。 |

:::note

- Merge Commit仅支持将**同构**导入请求合并到单个数据库和表中。“同构”表示 Stream Load 参数相同，包括：通用参数、JSON 格式参数、CSV 格式参数、`opt_properties` 和 Merge Commit 参数。
- 对于导入 CSV 格式的数据，您必须确保每行都以换行符结尾。`skip_header` 不支持。
- 服务器会自动为事务生成标签。如果指定了标签，则会忽略。
- Merge Commit 将多个导入请求合并到一个事务中。如果其中一个请求包含数据质量问题，则事务中的所有请求都将失败。

:::

### opt_properties

指定一些可选参数，这些参数应用于整个导入作业。语法：

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

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| label            | 否           | 用于指定导入作业的标签。如果您不指定标签，系统会自动为导入作业生成一个标签。相同标签的数据无法多次成功导入，这样可以避免一份数据重复导入。有关标签的命名规范，请参见[系统限制](../../System_limit.md)。系统默认保留最近 3 天内成功的导入作业的标签。您可以通过 [FE 配置参数](../../../administration/management/FE_configuration.md#导入导出) `label_keep_max_second` 设置默认保留时长。 |
| where            | 否           | 用于指定过滤条件。如果指定该参数，系统会按照指定的过滤条件对转换后的数据进行过滤。只有符合 WHERE 子句中指定的过滤条件的数据才会导入。 |
| max_filter_ratio | 否           | 用于指定导入作业的最大容错率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。取值范围：`0`~`1`。默认值：`0` 。<br />建议您保留默认值 `0`。这样的话，当导入的数据行中有错误时，导入作业会失败，从而保证数据的正确性。<br />如果希望忽略错误的数据行，可以设置该参数的取值大于 `0`。这样的话，即使导入的数据行中有错误，导入作业也能成功。<br />**说明**<br />这里因数据质量不合格而过滤掉的数据行，不包括通过 WHERE 子句过滤掉的数据行。 |
| log_rejected_record_num | 否           | 指定最多允许记录多少条因数据质量不合格而过滤掉的数据行数。该参数自 3.1 版本起支持。取值范围：`0`、`-1`、大于 0 的正整数。默认值：`0`。<ul><li>取值为 `0` 表示不记录过滤掉的数据行。</li><li>取值为 `-1` 表示记录所有过滤掉的数据行。</li><li>取值为大于 0 的正整数（比如 `n`）表示每个 BE（或 CN）节点上最多可以记录 `n` 条过滤掉的数据行。</li></ul> |
| timeout          | 否           | 用于导入作业的超时时间。取值范围：1 ~ 259200。单位：秒。默认值：`600`。<br />**说明**<br />除了 `timeout` 参数可以控制该导入作业的超时时间外，您还可以通过 [FE 配置参数](../../../administration/management/FE_configuration.md#导入导出) `stream_load_default_timeout_second` 来统一控制 Stream Load 导入作业的超时时间。如果指定了`timeout` 参数，则该导入作业的超时时间以 `timeout` 参数为准；如果没有指定 `timeout` 参数，则该导入作业的超时时间以`stream_load_default_timeout_second` 为准。 |
| strict_mode      | 否           | 用于指定是否开严格模式。取值范围：`true` 和 `false`。默认值：`false`。`true` 表示开启，`false` 表示关闭。<br />关于该模式的介绍，参见 [严格模式](../../../loading/load_concept/strict_mode.md)。|
| timezone         | 否           | 用于指定导入作业所使用的时区。默认为东八区 (Asia/Shanghai)。<br />该参数的取值会影响所有导入涉及的、跟时区设置有关的函数所返回的结果。受时区影响的函数有 strftime、alignment_timestamp 和 from_unixtime 等，具体请参见[设置时区](../../../administration/management/timezone.md)。导入参数 `timezone` 设置的时区对应“[设置时区](../../../administration/management/timezone.md)”中所述的会话级时区。 |
| load_mem_limit   | 否           | 导入作业的内存限制，最大不超过 BE（或 CN）的内存限制。单位：字节。默认内存限制为 2 GB。 |
| partial_update | 否 |是否使用部分列更新。取值包括 `TRUE` 和 `FALSE`。默认值：`FALSE`。|
| partial_update_mode | 否 | 指定部分更新的模式，取值包括 `row` 和 `column`。<ul><li>`row`（默认值），指定使用行模式执行部分更新，比较适用于较多列且小批量的实时更新场景。</li><li>`column`，指定使用列模式执行部分更新，比较适用于少数列并且大量行的批处理更新场景。在该场景，开启列模式，更新速度更快。例如，在一个包含 100 列的表中，每次更新 10 列（占比 10%）并更新所有行，则开启列模式，更新性能将提高 10 倍。</li></ul>|
| merge_condition  | 否           | 用于指定作为更新生效条件的列名。这样只有当导入的数据中该列的值大于等于当前值的时候，更新才会生效。v2.5 起支持条件更新。参见[通过导入实现数据变更](../../../loading/Load_to_Primary_Key_tables.md)。 <br/>**说明**<br/>指定的列必须为非主键列，且仅主键表支持条件更新。  |

## 列映射

### 配置 CSV 数据导入的列映射

如果数据文件的列可以按顺序与表的列一一对应，则无需配置数据文件与表之间的列映射。

如果数据文件的列不能按顺序与表的列一一对应，则需要使用 `columns` 参数配置数据文件与表之间的列映射。这包括以下两种使用场景：

- **列数相同但列顺序不同。** **此外，数据文件中的数据在导入到匹配的表列之前无需通过函数计算。**

  在 `columns` 参数中，您需要按照数据文件列的排列顺序指定表列的名称。

  例如，表包含三列，分别是 `col1`，`col2`，以及 `col3` 的顺序，并且数据文件也包含三列，这些列可以按顺序映射到表列 `col3`，`col2`，以及`col1`。在这种情况下，您需要指定`"columns: col3, col2, col1"`。

- **列数不同，列顺序也不同。此外，数据文件中的数据在导入到匹配的表列之前，需要通过函数进行计算。**

  在`columns`参数中，您需要按照数据文件列的排列顺序指定表列的名称，并指定用于计算数据的函数。以下是两个示例：

  - 该表包含三列，分别是 `col1`，`col2`，以及 `col3`。数据文件包含四列，其中前三列可以按顺序映射到表列 `col1`，`col2`，以及 `col3`，第四列不能映射到任何表列。在这种情况下，您需要为数据文件的第四列临时指定一个名称，并且该临时名称必须与任何表列名称不同。例如，您可以指定 `"columns: col1, col2, col3, temp"`，其中数据文件的第四列临时命名为 `temp`。
  - 该表包含三列，分别是 `year`，`month`，以及 `day`。数据文件只包含一列，该列以 `yyyy-mm-dd hh:mm:ss` 格式容纳日期和时间值。在这种情况下，您可以指定 `"columns: col, year = year(col), month=month(col), day=day(col)"`，其中 `col` 是数据文件列的临时名称，函数 `year = year(col)`，`month=month(col)`，以及 `day=day(col)` 用于从数据文件列 `col` 中提取数据并将其导入到映射的表列中。例如，`year = year(col)` 用于从数据文件列 `col` 提取 `yyyy` 数据并导入数据到表列 `year`。

有关详细示例，请参阅[配置列映射](#configure-column-mapping)。

### 配置 JSON 数据导入的列映射

如果 JSON 文档的键与表的列名相同，您可以使用简单模式导入 JSON 格式的数据。在简单模式下，您无需指定 `jsonpaths` 参数。此模式要求 JSON 格式的数据必须是一个对象，由花括号表示 `{}`，例如 `{"category": 1, "author": 2, "price": "3"}`。在此示例中，`category`、`author` 和 `price` 是键名，这些键可以按名称与表的列 `category`、`author` 和 `price` 一对一映射。

如果 JSON 文档的键名与表的列名不同，您可以使用匹配模式导入 JSON 格式的数据。在匹配模式下，您需要使用`jsonpaths`和`COLUMNS`参数来指定 JSON 文档和表之间的列映射：

- 在 `jsonpaths` 参数中，按照 JSON 文档中键的排列顺序指定 JSON 键。
- 在 `COLUMNS` 参数中，指定 JSON 键和表列之间的映射：
  - 在 `COLUMNS` 参数中指定的列名按顺序与 JSON 键一对一映射。
  - 在 `COLUMNS` 参数中指定的列名按名称与表列一对一映射。

有关使用匹配模式导入 JSON 格式数据的示例，请参见[使用匹配模式导入 JSON 数据](#load-json-data-using-matched-mode)。

## 返回值

导入作业完成后，系统以 JSON 格式返回作业结果。例如：

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

| **参数名称**           | **说明**                                                     |
| ---------------------- | ------------------------------------------------------------ |
| TxnId                  | 导入作业的事务 ID。                                          |
| Label                  | 导入作业的标签。                                             |
| Status                 | 此次导入的数据的最终状态。<ul><li>`Success`：表示数据导入成功，数据已经可见。</li><li>`Publish Timeout`：表示导入作业已经成功提交，但是由于某种原因数据并不能立即可见。可以视作已经成功、不必重试导入。</li><li>`Label Already Exists`：表示该标签已经被其他导入作业占用。数据可能导入成功，也可能是正在导入。</li><li>`Fail`：表示数据导入失败。您可以指定标签重试该导入作业。</li></ul> |
| Message                | 导入作业的状态详情。如果导入作业失败，这里会返回具体的失败原因。 |
| NumberTotalRows        | 读取到的总行数。                                             |
| NumberLoadedRows       | 成功导入的总行数。只有当返回结果中的 `Status` 为 `Success` 时有效。 |
| NumberFilteredRows     | 导入过程中因数据质量不合格而过滤掉的行数。                   |
| NumberUnselectedRows   | 导入过程中根据 WHERE 子句指定的条件而过滤掉的行数。          |
| LoadBytes              | 此次导入的数据量大小。单位：字节 (Bytes)。                   |
| LoadTimeMs             | 此次导入所用的时间。单位：毫秒 (ms)。                        |
| BeginTxnTimeMs         | 导入作业开启事务的时长。                                     |
| StreamLoadPlanTimeMs   | 导入作业生成执行计划的时长。                                 |
| ReadDataTimeMs         | 导入作业读取数据的时长。                                     |
| WriteDataTimeMs        | 导入作业写入数据的时长。                                     |
| CommitAndPublishTimeMs | 导入作业提交和数据发布的耗时。                               |

如果导入作业失败，系统还会返回`ErrorURL`。例如：

```JSON
{"ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"}
```

`ErrorURL` 提供一个 URL，您可以从中获取有关已过滤掉的不合格数据记录的详细信息。您可以使用可选参数 `log_rejected_record_num` 来指定可记录的不合格数据行的最大数量，该参数在您提交导入作业时设置。

您可以运行 `curl "url"` 直接查看已过滤掉的不合格数据记录的详细信息。您还可以运行 `wget "url"` 导出这些数据记录的详细信息：

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

导出的数据记录详情将保存到本地文件中，文件名类似于 `_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be`。您可以使用 `cat` 命令查看文件。

然后，您可以调整导入作业的配置，并再次提交导入作业。

## 示例

<TableURLTip />

### 导入 CSV 数据

本节以 CSV 数据为例，介绍如何通过各种参数设置和组合来满足您多样化的导入需求。

#### 设置超时时间

您的数据库 `test_db` 包含一个名为 `table1` 的表。该表包含三列，分别是 `col1`、`col2` 和 `col3` 依次排列。

您的数据文件 `example1.csv` 也包含三列，可以依次映射到 `col1`、`col2` 和 `col3` 的 `table1` 中。

如果您想将所有数据从 `example1.csv` 导入到 `table1` 中，且最长不超过 100 秒，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    <table_url_prefix>/api/test_db/table1/_stream_load
```

#### 设置错误容忍度

您的数据库 `test_db` 包含一个名为 `table2` 的表。该表包含三列，分别是 `col1`、`col2` 和 `col3` 依次排列。

您的数据文件 `example2.csv` 也包含三列，可以依次映射到 `col1`、`col2` 和 `col3` 的 `table2` 中。

如果您想将所有数据从 `example2.csv` 导入到 `table2` 中，且最大错误容忍度为 `0.2`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    <table_url_prefix>/api/test_db/table2/_stream_load
```

#### 配置列映射

您的数据库 `test_db` 包含一个名为 `table3` 的表。该表包含三列，分别为 `col1`、`col2` 和 `col3`，按此顺序排列。

您的数据文件 `example3.csv` 也包含三列，可以按顺序映射到 `col2`、`col1` 和 `col3` 的 `table3`中。

如果您想将所有数据从 `example3.csv` 导入到 `table3` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    <table_url_prefix>/api/test_db/table3/_stream_load
```

:::note
在上述示例中，`example3.csv` 的列无法按其在 `table3` 中排列的相同顺序映射到 `table3` 的列。因此，您需要使用 `columns` 参数来配置 `example3.csv` 和 `table3` 之间的列映射。
:::

#### 设置过滤条件

您的数据库 `test_db` 包含一个名为 `table4` 的表。该表包含三列，分别为 `col1`、`col2` 和 `col3`，按此顺序排列。

您的数据文件 `example4.csv` 也包含三列，可以按顺序映射到 `col1`、`col2` 和 `col3` 的 `table4` 中。

如果您只想将 `example4.csv` 的第一列中值等于 `20180601` 的数据记录导入到 `table4` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2，col3"\
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

如果您想将所有数据从 `example5.csv` 导入到分区 `p1` 和 `p2` 的 `table5` 中，请运行以下命令：

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

如果您想将所有数据从 `example6.csv` 导入到 `table6` 中，并使用严格模式和时区 `Africa/Abidjan`，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    <table_url_prefix>/api/test_db/table6/_stream_load
```

#### 将数据导入到包含 HLL 类型列的表中

您的数据库 `test_db` 包含一个名为 `table7` 的表。该表包含两个 HLL 类型列 `col1` 和 `col2`（按顺序）。

您的数据文件 `example7.csv` 也包含两列，其中第一列可以映射到 `col1` 的 `table7`，第二列不能映射到 `table7` 的任何列。在 `example7.csv` 的第一列中的值可以在导入到 `col1` 的 `table7` 之前，通过使用函数转换为 HLL 类型数据。

如果您想将数据从 `example7.csv` 导入到 `table7` 中，请运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    <table_url_prefix>/api/test_db/table7/_stream_load
```

:::note
在前面的示例中，通过使用 `columns` 参数，`example7.csv` 的两列被命名为 `temp1` 和 `temp2`（按顺序）。然后，使用函数转换数据，如下所示：

- 函数用于将 `hll_hash` 中的值转换为 HLL 类型数据，并将 `temp1` 的 `example7.csv` 映射到 `temp1` 的 `example7.csv` 的 `col1`。
- 函数用于将指定的默认值填充到 `hll_empty` 的 `col2` 中。
:::`table7`。

有关函数 `hll_hash` 和 `hll_empty` 的用法，请参阅 [hll_hash](../../sql-functions/scalar-functions/hll_hash.md) 和 [hll_empty](../../sql-functions/scalar-functions/hll_empty.md)。

#### 将数据导入到包含 BITMAP 类型列的表中

您的数据库 `test_db` 包含一个名为 `table8` 的表。该表包含两个 BITMAP 类型的列，分别是 `col1` 和 `col2`，按顺序排列。

您的数据文件 `example8.csv` 也包含两列，其中第一列可以映射到 `col1` 的 `table8`，第二列不能映射到 `table8` 的任何列。 `example8.csv` 的第一列中的值可以在导入到 `col1` 的 `table8` 之前使用函数进行转换。

如果要将数据从 `example8.csv` 导入到 `table8` 中，请运行以下命令：

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
:::

有关函数 `to_bitmap` 和 `bitmap_empty` 的用法，请参见 [to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) 和 [bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md)。

#### 设置 `skip_header`，`trim_space`，`enclose`，和 `escape`

您的数据库 `test_db` 包含一个名为 `table9` 的表。该表包含三列，分别是 `col1`，`col2`，和 `col3`，按顺序排列。

您的数据文件 `example9.csv` 也包含三列，它们按顺序映射到 `col2`，`col1`，和 `col3` 的 `table13`。

如果您想将所有数据从 `example9.csv` 导入到 `table9` 中，并跳过 `example9.csv` 的前五行，删除列分隔符前后空格，并将 `enclose` 设置为 `\`，将 `escape` 设置为 `\`，请运行以下命令：

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

如果您想将所有数据从 `example10.csv` 导入到 `table10` 中，运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label10" \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "row_delimiter:\\x0D0A" \
    -T example10.csv -XPUT \
    <table_url_prefix>/api/test_db/table10/_stream_load
```

### 导入 JSON 数据

本节介绍导入 JSON 数据时需要注意的参数设置。

您的数据库 `test_db` 包含一个名为 `tbl1` 的表，其 schema 如下：

```SQL
`category` varchar(512) NULL COMMENT "",`author` varchar(512) NULL COMMENT "",`title` varchar(512) NULL COMMENT "",`price` double NULL COMMENT ""
```

#### 使用简单模式导入 JSON 数据

假设您的数据文件`example1.json`包含以下数据：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

要将所有数据从`example1.json`导入到`tbl1`中，运行以下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    <table_url_prefix>/api/test_db/tbl1/_stream_load
```

:::note
在前面的示例中，参数 `columns` 和 `jsonpaths` 未指定。因此，`example1.json` 中的键按名称映射到 `tbl1` 的列。
:::

为提高吞吐量，Stream Load 支持一次性导入多条数据记录。例如：

```JSON
[{"category":"C++","author":"avc","title":"C++ primer","price":89.5},{"category":"Java","author":"avc","title":"Effective Java","price":95},{"category":"Linux","author":"avc","title":"Linux kernel","price":195}]
```

#### 使用匹配模式导入 JSON 数据

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

##### 使用匹配模式导入 JSON 数据（未指定根元素）

假设您的数据文件 `example2.json` 包含以下数据：

```JSON
[{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}]
```

要仅导入`example2.json` 中的 `category`、`author` 和 `price`，运行以下命令：

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
在前面的示例中，JSON 数据的最外层是一个数组结构，由一对方括号表示`[]`。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将 `strip_outer_array` 设置为 `true` 以剥离最外层的数组结构。**title**键无需导入，将在导入时被忽略。
:::

##### 使用匹配模式导入指定根元素的 JSON 数据

假设您的数据文件 `example3.json` 包含以下数据：

```JSON
{"id": 10001,"RECORDS":[{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},{"category":"22","author":"2avc","price":895,"timestamp":1589191487},{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}],"comments": ["3 records", "there will be 3 rows"]}
```

要从 `example3.json` 中导入 `category`、`author` 和 `price`，运行以下命令：

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
在前面的示例中，JSON 数据的最外层是一个数组结构，由一对方括号表示`[]`。该数组结构由多个 JSON 对象组成，每个对象代表一条数据记录。因此，您需要将`strip_outer_array`设置为`true`以剥离最外层的数组结构。`title` 和 `timestamp` 键无需导入，在导入时会被忽略。此外，`json_root` 参数用于指定 JSON 数据的根元素（即一个数组）。
:::

### 合并 Stream Load 请求

- 运行以下命令以同步模式启动一个启用 Merge Commit 的 Stream Load 作业，并将合并窗口设置为 `5000` 毫秒，并将并行度设置为 `2`：

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

- 运行以下命令以异步模式启动一个启用 Merge Commit 的 Stream Load 作业，并将合并窗口设置为 `60000` 毫秒，并将并行度设置为 `2`：

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
