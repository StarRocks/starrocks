---
displayed_sidebar: "Chinese"
---

# STREAM LOAD

## 功能

Stream Load 是一种基于 HTTP 协议的同步导入方式，支持将本地文件或数据流导入到 StarRocks 中。您提交导入作业以后，StarRocks 会同步地执行导入作业，并返回导入作业的结果信息。您可以通过返回的结果信息来判断导入作业是否成功。有关 Stream Load 的应用场景、使用限制、基本原理、以及支持的数据文件格式等信息，请参见[通过 HTTP Push 从本地文件系统或流式数据源导入数据](../../../loading/StreamLoad.md)。

> **注意**
>
> - Stream Load 操作会同时更新和 StarRocks 原始表相关的物化视图的数据。
> - Stream Load 操作需要目标表的 INSERT 权限。如果您的用户账号没有 INSERT 权限，请参考 [GRANT](../account-management/GRANT.md) 给用户赋权。

## 语法

```Bash
curl --location-trusted -u <username>:<password> -XPUT <url>
(
    data_desc
)
[opt_properties]        
```

本文以 curl 工具为例介绍如何使用 Stream Load 导入数据。除了使用 curl 工具，您还可以通过其他支持 HTTP 协议的工具或语言提交导入作业以导入数据。导入相关的参数位于 HTTP 请求的请求头。传入这些导入相关的参数时，需要注意以下几点：

- 推荐使用**分块上传**方式，如本文示例所示。如果使用**非分块上传**方式，则必须使用请求头字段 `Content-Length` 来标示待上传内容的长度，从而保证数据完整性。

  > **说明**
  >
  > 使用 curl 工具提交导入作业的时候，会自动添加 `Content-Length` 字段，因此无需手动指定 `Content-Length`。

- 必须在 HTTP 请求的请求头字段 `Expect` 中指定 `100-continue`，即 `"Expect:100-continue"`。这样在服务器拒绝导入作业请求的情况下，可以避免不必要的数据传输，从而减少不必要的资源开销。

注意在 StarRocks 中，部分文字是 SQL 语言的保留关键字，不能直接用于 SQL 语句。如果想在 SQL 语句中使用这些保留关键字，必须用反引号 (`) 包含起来。参见[关键字](/sql-reference/sql-statements/keywords.md)。

## 参数说明

### username 和 password

用于指定 StarRocks 集群账号的用户名和密码。必选参数。如果账号没有设置密码，这里只需要传入 `<username>:`。

### XPUT

用于指定 HTTP 请求方法。必选参数。Stream Load 当前只支持 PUT 方法。

### url

用于指定 StarRocks 表的 URL 地址。必选参数。语法如下：

```Plain
http://<fe_host>:<fe_http_port>/api/<database_name>/<table_name>/_stream_load
```

`url` 中的参数如下表所述。

| 参数名称      | 是否必须 | 参数说明                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| fe_host       | 是       | 指定 StarRocks 集群中 FE 的 IP 地址。 <br />**说明**<br />如果您直接提交导入作业给某一个 BE 节点，则需要传入该 BE 的 IP 地址。 |
| fe_http_port  | 是       | 指定 StarRocks 集群中 FE 的 HTTP 端口号。 默认端口号为 8030。 <br />**说明**<br />如果您直接提交导入作业给某一指定的 BE 节点，则需要传入该 BE 的 HTTP 端口号。默认端口号为 8040。 |
| database_name | 是       | 指定目标 StarRocks 表所在的数据库的名称。                    |
| table_name    | 是       | 指定目标 StarRocks 表的名称。                                |

> **说明**
>
> 您可以通过 [SHOW FRONTENDS](../Administration/SHOW_FRONTENDS.md) 命令查看 FE 节点的 IP 地址和 HTTP 端口号。

### data_desc

用于描述源数据文件，包括源数据文件的名称、格式、列分隔符、行分隔符、目标分区、以及与 StarRocks 表之间的列对应关系等。语法如下：

```Bash
-T <file_path>
-H "format: CSV | JSON"
-H "column_separator: <column_separator>"
-H "row_delimiter: <row_delimiter>"
-H "columns: <column1_name>[, <column2_name>，... ]"
-H "partitions: <partition1_name>[, <partition2_name>, ...]"
-H "temporary_partitions: <temporary_partition1_name>[, <temporary_partition2_name>, ...]"
-H "jsonpaths: [ \"<json_path1>\"[, \"<json_path2>\", ...] ]"
-H "strip_outer_array:  true | false"
-H "json_root: <json_path>"
```

`data_desc` 中的参数可以分为三类：公共参数、CSV 适用的参数、以及 JSON 适用的参数。

#### 公共参数

| **参数名称** | **是否必选** | **参数说明**                                                 |
| ------------ | ------------ | ------------------------------------------------------------ |
| file_path    | 是           | 指定源数据文件的保存路径。文件名里可选包含或者不包含扩展名。 |
| format       | 否           | 指定待导入数据的格式。取值包括 `CSV` 和 `JSON`。默认值：`CSV`。 |
| partitions   | 否           | 指定要把数据导入哪些分区。如果不指定该参数，则默认导入到 StarRocks 表所在的所有分区中。 |
| temporary_partitions | 否           | 指定要把数据导入哪些[临时分区](../../../table_design/Temporary_partition.md)。|
| columns      | 否           | 指定源数据文件和 StarRocks 表之间的列对应关系。如果源数据文件中的列与 StarRocks 表中的列按顺序一一对应，则不需要指定 `columns` 参数。您可以通过 `columns` 参数实现数据转换。例如，要导入一个 CSV 格式的数据文件，文件中有两列，分别可以对应到目标 StarRocks 表的 `id` 和 `city` 两列。如果要实现把数据文件中第一列的数据乘以 100 以后再落入 StarRocks 表的转换，可以指定 `"columns: city,tmp_id, id = tmp_id * 100"`。具体请参见本文“[列映射](#列映射)”章节。 |

#### CSV 适用参数

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| column_separator | 否           | 用于指定源数据文件中的列分隔符。如果不指定该参数，则默认为 `\t`，即 Tab。必须确保这里指定的列分隔符与源数据文件中的列分隔符一致。<br />**说明**<br />StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (\|)。 |
| row_delimiter    | 否           | 用于指定源数据文件中的行分隔符。如果不指定该参数，则默认为 `\n`。 |
| skip_header      | 否           | 用于指定跳过 CSV 文件最开头的几行数据。取值类型：INTEGER。默认值：`0`。<br />在某些 CSV 文件里，最开头的几行数据会用来定义列名、列类型等元数据信息。通过设置该参数，可以使 StarRocks 在导入数据时忽略 CSV 文件的前面几行。例如，如果设置该参数为 `1`，则 StarRocks 会在导入数据时忽略 CSV 文件的第一行。<br />这里的行所使用的分隔符须与您在导入命令中所设定的行分隔符一致。 |
| trim_space       | 否           | 用于指定是否去除 CSV 文件中列分隔符前后的空格。取值类型：BOOLEAN。默认值：`false`。<br />有些数据库在导出数据为 CSV 文件时，会在列分隔符的前后添加一些空格。根据位置的不同，这些空格可以称为“前导空格”或者“尾随空格”。通过设置该参数，可以使 StarRocks 在导入数据时删除这些不必要的空格。<br />需要注意的是，StarRocks 不会去除被 `enclose` 指定字符括起来的字段内的空格（包括字段的前导空格和尾随空格）。例如，列分隔符是竖线 (<code class="language-text">&#124;</code>)，`enclose` 指定的字符是双引号 (`"`)：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />如果设置 `trim_space` 为 `true`，则 StarRocks 处理后的结果数据如下：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose          | 否           | 根据 [RFC4180](https://www.rfc-editor.org/rfc/rfc4180)，用于指定把 CSV 文件中的字段括起来的字符。取值类型：单字节字符。默认值：`NONE`。最常用 `enclose` 字符为单引号 (`'`) 或双引号 (`"`)。<br />被 `enclose` 指定字符括起来的字段内的所有特殊字符（包括行分隔符、列分隔符等）均看做是普通符号。比 RFC4180 标准更进一步的是，StarRocks 提供的 `enclose` 属性支持设置任意单个字节的字符。<br />如果一个字段内包含了 `enclose` 指定字符，则可以使用同样的字符对 `enclose` 指定字符进行转义。例如，在设置了`enclose` 为双引号 (`"`) 时，字段值 `a "quoted" c` 在 CSV 文件中应该写作 `"a ""quoted"" c"`。 |
| escape           | 否           | 指定用于转义的字符。用来转义各种特殊字符，比如行分隔符、列分隔符、转义符、`enclose` 指定字符等，使 StarRocks 把这些特殊字符当做普通字符而解析成字段值的一部分。取值类型：单字节字符。默认值：`NONE`。最常用的 `escape` 字符为斜杠 (`\`)，在 SQL 语句中应该写作双斜杠 (`\\`)。<br />**说明**<br />`escape` 指定字符同时作用于 `enclose` 指定字符的内部和外部。<br />以下为两个示例：<ul><li>当设置 `enclose` 为双引号 (`"`) 、`escape` 为斜杠 (`\`) 时，StarRocks 会把 `"say \"Hello world\""` 解析成一个字段值 `say "Hello world"`。</li><li>假设列分隔符为逗号 (`,`) ，当设置 `escape` 为斜杠 (`\`) ，StarRocks 会把 `a, b\, c` 解析成 `a` 和 `b, c` 两个字段值。</li></ul> |

> **说明**
>
> 对于 CSV 格式的数据，需要注意以下两点：
>
> - StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。
> - 空值 (null) 用 `\N` 表示。比如，数据文件一共有三列，其中某行数据的第一列、第三列数据分别为 `a` 和 `b`，第二列没有数据，则第二列需要用 `\N` 来表示空值，写作 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示第二列是一个空字符串。
> - `skip_header`、`trim_space`、`enclose` 和 `escape` 等属性在 3.0 及以后版本支持。

#### JSON 适用参数

| **参数名称**      | **是否必选** | **参数说明**                                                 |
| ----------------- | ------------ | ------------------------------------------------------------ |
| jsonpaths         | 否           | 用于指定待导入的字段的名称。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为 JSON 格式。参见[导入 JSON 数据时配置列映射关系](#导入-json-数据时配置列映射关系).     |
| strip_outer_array | 否           | 用于指定是否裁剪最外层的数组结构。取值范围：`true` 和 `false`。默认值：`false`。真实业务场景中，待导入的 JSON 数据可能在最外层有一对表示数组结构的中括号 `[]`。这种情况下，一般建议您指定该参数取值为 `true`，这样 StarRocks 会剪裁掉外层的中括号 `[]`，并把中括号 `[]` 里的每个内层数组都作为一行单独的数据导入。如果您指定该参数取值为 `false`，则 StarRocks 会把整个 JSON 数据文件解析成一个数组，并作为一行数据导入。例如，待导入的 JSON 数据为 `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]`，如果指定该参数取值为 `true`，则 StarRocks 会把 `{"category" : 1, "author" : 2}` 和 `{"category" : 3, "author" : 4}` 解析成两行数据，并导入到目标 StarRocks 表中对应的数据行。 |
| json_root         | 否           | 用于指定待导入 JSON 数据的根元素。仅在使用匹配模式导入 JSON 数据时需要指定该参数。参数取值为合法的 JsonPath 字符串。默认值为空，表示会导入整个 JSON 数据文件的数据。具体请参见本文提供的示例“[导入数据并指定 JSON 根节点](#指定-json-根节点使匹配模式导入数据)”。 |
| ignore_json_size | 否   | 用于指定是否检查 HTTP 请求中 JSON Body 的大小。<br />**说明**<br />HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。 |

另外，导入 JSON 格式的数据时，需要注意单个 JSON 对象的大小不能超过 4 GB。如果 JSON 文件中单个 JSON 对象的大小超过 4 GB，会提示 "This parser can't support a document that big." 错误。

### opt_properties

用于指定一些导入相关的可选参数。指定的参数设置作用于整个导入作业。语法如下：

```Bash
-H "label: <label_name>"
-H "where: <condition1>[, <condition2>, ...]"
-H "max_filter_ratio: <num>"
-H "timeout: <num>"
-H "strict_mode: true | false"
-H "timezone: <string>"
-H "load_mem_limit: <num>"
-H "merge_condition: <column_name>"
```

参数说明如下表所述。

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| label            | 否           | 用于指定导入作业的标签。如果您不指定标签，StarRocks 会自动为导入作业生成一个标签。相同标签的数据无法多次成功导入，这样可以避免一份数据重复导入。有关标签的命名规范，请参见[系统限制](../../../reference/System_limit.md)。StarRocks 默认保留最近 3 天内成功的导入作业的标签。您可以通过 [FE 配置参数](../../../administration/Configuration.md#导入和导出) `label_keep_max_second` 设置默认保留时长。 |
| where            | 否           | 用于指定过滤条件。如果指定该参数，StarRocks 会按照指定的过滤条件对转换后的数据进行过滤。只有符合 WHERE 子句中指定的过滤条件的数据才会导入。 |
| max_filter_ratio | 否           | 用于指定导入作业的最大容错率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。取值范围：`0`~`1`。默认值：`0` 。<br />建议您保留默认值 `0`。这样的话，当导入的数据行中有错误时，导入作业会失败，从而保证数据的正确性。<br />如果希望忽略错误的数据行，可以设置该参数的取值大于 `0`。这样的话，即使导入的数据行中有错误，导入作业也能成功。<br />**说明**<br />这里因数据质量不合格而过滤掉的数据行，不包括通过 WHERE 子句过滤掉的数据行。 |
| log_rejected_record_num | 否           | 指定最多允许记录多少条因数据质量不合格而过滤掉的数据行数。该参数自 3.1 版本起支持。取值范围：`0`、`-1`、大于 0 的正整数。默认值：`0`。<ul><li>取值为 `0` 表示不记录过滤掉的数据行。</li><li>取值为 `-1` 表示记录所有过滤掉的数据行。</li><li>取值为大于 0 的正整数（比如 `n`）表示每个 BE 节点上最多可以记录 `n` 条过滤掉的数据行。</li></ul> |
| timeout          | 否           | 用于导入作业的超时时间。取值范围：1 ~ 259200。单位：秒。默认值：`600`。<br />**说明**<br />除了 `timeout` 参数可以控制该导入作业的超时时间外，您还可以通过 [FE 配置参数](../../../administration/Configuration.md#导入和导出) `stream_load_default_timeout_second` 来统一控制 Stream Load 导入作业的超时时间。如果指定了`timeout` 参数，则该导入作业的超时时间以 `timeout` 参数为准；如果没有指定 `timeout` 参数，则该导入作业的超时时间以`stream_load_default_timeout_second` 为准。 |
| strict_mode      | 否           | 用于指定是否开严格模式。取值范围：`true` 和 `false`。默认值：`false`。`true` 表示开启，`false` 表示关闭。<br />关于该模式的介绍，参见 [严格模式](../../../loading/load_concept/strict_mode.md)。|
| timezone         | 否           | 用于指定导入作业所使用的时区。默认为东八区 (Asia/Shanghai)。<br />该参数的取值会影响所有导入涉及的、跟时区设置有关的函数所返回的结果。受时区影响的函数有 strftime、alignment_timestamp 和 from_unixtime 等，具体请参见[设置时区](../../../administration/timezone.md)。导入参数 `timezone` 设置的时区对应“[设置时区](../../../administration/timezone.md)”中所述的会话级时区。 |
| load_mem_limit   | 否           | 导入作业的内存限制，最大不超过 BE 的内存限制。单位：字节。默认内存限制为 2 GB。 |
| merge_condition  | 否           | 用于指定作为更新生效条件的列名。这样只有当导入的数据中该列的值大于等于当前值的时候，更新才会生效。StarRocks v2.5 起支持条件更新。参见[通过导入实现数据变更](../../../loading/Load_to_Primary_Key_tables.md)。 <br/>**说明**<br/>指定的列必须为非主键列，且仅主键模型表支持条件更新。  |

## 列映射

### 导入 CSV 数据时配置列映射关系

如果源数据文件中的列与目标表中的列按顺序一一对应，您不需要指定列映射和转换关系。

如果源数据文件中的列与目标表中的列不能按顺序一一对应，包括数量或顺序不一致，则必须通过 `COLUMNS` 参数来指定列映射和转换关系。一般包括如下两种场景：

- **列数量一致、但是顺序不一致，并且数据不需要通过函数计算、可以直接落入目标表中对应的列。** 这种场景下，您需要在 `COLUMNS` 参数中按照源数据文件中的列顺序、使用目标表中对应的列名来配置列映射和转换关系。

  例如，目标表中有三列，按顺序依次为 `col1`、`col2` 和 `col3`；源数据文件中也有三列，按顺序依次对应目标表中的 `col3`、`col2` 和 `col1`。这种情况下，需要指定 `COLUMNS(col3, col2, col1)`。

- **列数量、顺序都不一致，并且某些列的数据需要通过函数计算以后才能落入目标表中对应的列。** 这种场景下，您不仅需要在 `COLUMNS` 参数中按照源数据文件中的列顺序、使用目标表中对应的列名来配置列映射关系，还需要指定参与数据计算的函数。以下为两个示例：

  - 目标表中有三列，按顺序依次为 `col1`、`col2` 和 `col3` ；源数据文件中有四列，前三列按顺序依次对应目标表中的 `col1`、`col2` 和 `col3`，第四列在目标表中无对应的列。这种情况下，需要指定 `COLUMNS(col1, col2, col3, temp)`，其中，最后一列可随意指定一个名称（如 `temp`）用于占位即可。
  - 目标表中有三列，按顺序依次为 `year`、`month` 和 `day`。源数据文件中只有一个包含时间数据的列，格式为 `yyyy-mm-dd hh:mm:ss`。这种情况下，可以指定 `COLUMNS (col, year = year(col), month=month(col), day=day(col))`。其中，`col` 是源数据文件中所包含的列的临时命名，`year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于指定从 `col` 列提取对应的数据并落入目标表中对应的列，如 `year = year(col)` 表示通过 `year` 函数提取源数据文件中 `col` 列的 `yyyy` 部分的数据并落入目标表中的 `year` 列。

有关操作示例，参见[设置列映射关系](#设置列映射关系)。

### 导入 JSON 数据时配置列映射关系

如果 JSON 文件中的 Key 名与目标表中的列名一致，您可以使用简单模式来导入数据。简单模式下，不需要设置 `jsonpaths` 参数，这种模式要求 JSON 数据是大括号 {} 表示的对象类型，例如 `{"category": 1, "author": 2, "price": "3"}` 中，`category`、`author`、`price` 是 Key 的名称，按名称直接对应目标 表中的 `category`、`author`、`price` 三列。

如果 JSON 文件中的 Key 名与目标表中的列名不一致，则需要使用匹配模式来导入数据。匹配模式下，需要通过 `jsonpaths` 和 `COLUMNS` 两个参数来指定 JSON 文件中的 Key 和目标表中的列之间的映射和转换关系：

- `jsonpaths` 参数中按照 JSON 文件中 Key 的顺序一一指定待导入的 Key。
- `COLUMNS` 参数中指定 JSON 文件中的 Key 与目标表中的列之间的映射关系和数据转换关系。
  - `COLUMNS` 参数中指定的列名与 `jsonpaths` 参数中指定的 Key 按顺序保持一一对应。
  - `COLUMNS` 参数中指定的列名与目标表中的列按名称保持一一对应。

有关使用匹配模式导入 JSON 数据的示例，参见[使用匹配模式导入数据](#使用匹配模式导入数据)。

## 返回值

导入结束后，会以 JSON 格式返回导入作业的结果信息，如下所示：

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

返回结果中的参数说明如下表所述。

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

如果导入作业失败，还会返回 `ErrorURL`，如下所示：

```JSON
{
    "ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"
}
```

通过 `ErrorURL` 可以查看导入过程中因数据质量不合格而过滤掉的错误数据行的具体信息。您可以在提交导入作业时，通过可选参数 `log_rejected_record_num` 来指定最多可以记录多少条错误数据行的信息。

您可以通过  `curl "url"` 命令直接查看错误数据行的信息。也可以通过 `wget "url"` 命令导出错误数据行的信息，如下所示：

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

导出的错误数据行信息会保存到一个名为 `_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` 的本地文件中。您可以通过 `cat _load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` 命令查看该文件的内容。

您可以根据错误信息调整导入作业，然后重新提交导入作业。

## 示例

### 导入 CSV 格式的数据

本小节以 CSV 格式的数据为例，重点阐述在创建导入作业的时候，如何运用各种参数配置来满足不同业务场景下的各种导入要求。

#### **设置超时时间**

StarRocks 数据库 `test_db` 里的表 `table1` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example1.csv` 也包含三列，按顺序一一对应 `table1` 中的三列 `col1`、`col2`、`col3`。

如果要把 `example1.csv` 中所有的数据都导入到 `table1` 中，并且要求超时时间最大不超过 100 秒，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label1" \
    -H "Expect:100-continue" \
    -H "timeout:100" \
    -H "max_filter_ratio:0.2" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

#### **设置最大容错率**

StarRocks 数据库 `test_db` 里的表 `table2` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example2.csv` 也包含三列，按顺序一一对应 `table2` 中的三列 `col1`、`col2`、`col3`。

如果要把 `example2.csv` 中所有的数据都导入到 `table2` 中，并且要求容错率最大不超过 `0.2`，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label2" \
    -H "Expect:100-continue" \
    -H "max_filter_ratio:0.2" \
    -T example2.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

#### **设置列映射关系**

StarRocks 数据库 `test_db` 里的表 `table3` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example3.csv` 也包含三列，按顺序依次对应 `table3` 中 `col2`、`col1`、`col3`。

如果要把 `example3.csv` 中所有的数据都导入到 `table3` 中，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label3" \
    -H "Expect:100-continue" \
    -H "columns: col2, col1, col3" \
    -T example3.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table3/_stream_load
```

> **说明**
>
> 上述示例中，因为 `example3.csv` 和 `table3` 所包含的列不能按顺序依次对应，因此需要通过 `columns` 参数来设置 `example3.csv` 和 `table3` 之间的列映射关系。

#### **设置筛选条件**

StarRocks 数据库 `test_db` 里的表 `table4` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example4.csv` 也包含三列，按顺序一一对应 `table4` 中的三列 `col1`、`col2`、`col3`。

如果只想把 `example4.csv` 中第一列的值等于 `20180601` 的数据行导入到 `table4` 中，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label4" \
    -H "Expect:100-continue" \
    -H "columns: col1, col2，col3]"\
    -H "where: col1 = 20180601" \
    -T example4.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table4/_stream_load
```

> **说明**
>
> 上述示例中，虽然 `example4.csv` 和 `table4` 所包含的列数目相同、并且按顺序一一对应，但是因为需要通过 WHERE 子句指定基于列的过滤条件，因此需要通过 `columns` 参数对 `example4.csv` 中的列进行临时命名。

#### **设置目标分区**

StarRocks 数据库 `test_db` 里的表 `table5` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example5.csv` 也包含三列，按顺序一一对应 `table5` 中的三列 `col1`、`col2`、`col3`。

如果要把 `example5.csv` 中所有的数据都导入到 `table5` 所在的分区 `p1` 和 `p2`，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password>  -H "label:label5" \
    -H "Expect:100-continue" \
    -H "partitions: p1, p2" \
    -T example5.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table5/_stream_load
```

#### **设置严格模式和时区**

StarRocks 数据库 `test_db` 里的表 `table6` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example6.csv` 也包含三列，按顺序一一对应 `table6` 中的三列 `col1`、`col2`、`col3`。

如果要把 `example6.csv` 中所有的数据导入到 `table6` 中，并且要求进严格模式的过滤、使用时区 `Africa/Abidjan`，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "strict_mode: true" \
    -H "timezone: Africa/Abidjan" \
    -T example6.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table6/_stream_load
```

#### **导入数据到含有 HLL 类型列的表**

StarRocks 数据库 `test_db` 里的表 `table7` 包含两个 HLL 类型的列，按顺序依次为 `col1`、`col2`。

数据文件 `example7.csv` 也包含两列，第一列对应 `table7` 中  HLL 类型的列`col1`，可以通过函数转换成 HLL 类型的数据并落入 `col1` 列；第二列跟 `table7` 中任何一列都不对应。

如果要把 `example7.csv` 中对应的数据导入到 `table7` 中，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=hll_hash(temp1), col2=hll_empty()" \
    -T example7.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table7/_stream_load
```

> **说明**
>
> 上述示例中，通过 `columns` 参数，把 `example7.csv` 中的两列临时命名为 `temp1`、`temp2`，然后使用函数指定数据转换规则，包括：
>
> - 使用 `hll_hash` 函数把 `example7.csv` 中的 `temp1` 列转换成 HLL 类型的数据并映射到 `table7`中的 `col1` 列。
>
> - 使用 `hll_empty` 函数给导入的数据行在 `table7` 中的第二列补充默认值。

有关 `hll_hash` 函数和 `hll_empty` 函数的用法，请参见 [hll_hash](../../sql-functions/aggregate-functions/hll_hash.md) 和 [hll_empty](../../sql-functions/aggregate-functions/hll_empty.md)。

#### **导入数据到含有 BITMAP 类型列的表**

StarRocks 数据库 `test_db` 里的表 `table8` 包含两个 BITMAP 类型的列，按顺序依次为 `col1`、`col2`。

数据文件 `example8.csv` 也包含两列，第一列对应 `table8` 中 BITMAP 类型的列 `col1`，可以通过函数转换成 BITMAP 类型的数据并落入 `col1` 列；第二列跟 `table8` 中任何一列都不对应。

如果要把 `example8.csv` 中对应的数据导入到 `table8` 中，可以执行如下命令：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "columns: temp1, temp2, col1=to_bitmap(temp1), col2=bitmap_empty()" \
    -T example8.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table8/_stream_load
```

> **说明**
>
> 上述示例中，通过 `columns` 参数，把 `example8.csv` 中的两列临时命名为 `temp1`、`temp2`，然后使用函数指定数据转换规则，包括：
>
> - 使用 `to_bitmap` 函数把 `example8.csv` 中的 `temp1` 列转换成 BITMAP 类型的数据并落入 `table8` 中的 `col1` 列。
>
> - 使用 `bitmap_empty` 函数给导入的数据行在 `table8` 中的第二列补充默认值。

有关 `to_bitmap` 函数和 `bitmap_empty` 函数的用法，请参见 [to_bitmap](../../sql-functions/bitmap-functions/to_bitmap.md) 和 [bitmap_empty](../../sql-functions/bitmap-functions/bitmap_empty.md)。

#### 设置 `skip_header`、`trim_space`、`enclose` 和 `escape`

StarRocks 数据库 `test_db` 里的表 `table9` 包含三列，按顺序依次为 `col1`、`col2`、`col3`。

数据文件 `example9.csv` 也包含三列，按顺序依次对应 `table9` 中 `col2`、`col1`、`col3`。

如果要把 `example9.csv` 中所有的数据都导入到 `table9` 中，并且希望跳过 `example9.csv` 中最开头的五行数据、去除列分隔符前后的空格、 指定 `enclose` 字符为斜杠 (`\`)、并且指定 `escape` 字符为斜杠 (`\`)，可以执行如下语句：

```Bash
curl --location-trusted -u <username>:<password> -H "label:3875" \
    -H "Expect:100-continue" \
    -H "trim_space: true" -H "skip_header: 5" \
    -H "column_separator:," -H "enclose:\"" -H "escape:\\" \
    -H "columns: col2, col1, col3" \
    -T example9.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl9/_stream_load
```

### **导入 JSON 格式的数据**

本小节主要描述导入 JSON 格式的数据时，需要关注的一些参数配置。

StarRocks 数据库 `test_db` 里的表 `tbl1` 拥有如下表结构：

```SQL
`category` varchar(512) NULL COMMENT "",
`author` varchar(512) NULL COMMENT "",
`title` varchar(512) NULL COMMENT "",
`price` double NULL COMMENT ""
```

#### **使用简单模式导入数据**

假设数据文件 `example1.json` 包含如下数据：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

您可以通过如下命令将 `example1.json` 中的数据导入到 `tbl1` 中：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label6" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -T example1.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **说明**
>
> 如上述示例所示，在没有指定 `columns` 和 `jsonpaths` 参数的情况下，则会按照 StarRocks 表中的列名称去对应 JSON 数据文件中的字段。

为了提升吞吐量，Stream Load 支持一次性导入多条数据。比如，可以一次性导入 JSON 数据文件中如下多条数据：

```JSON
[
    {"category":"C++","author":"avc","title":"C++ primer","price":89.5},
    {"category":"Java","author":"avc","title":"Effective Java","price":95},
    {"category":"Linux","author":"avc","title":"Linux kernel","price":195}
]
```

#### **使用匹配模式导入数据**

StarRocks 按照如下顺序对数据进行匹配和处理：

1. （可选）根据 `strip_outer_array` 参数裁剪最外层的数组结构。

    > **说明**
    >
    > 仅在 JSON 数据的最外层是一个通过中括号 `[]` 表示的数组结构时涉及，需要设置 `strip_outer_array` 为 `true`。

2. （可选）根据 `json_root` 参数匹配 JSON 数据的根节点。

    > **说明**
    >
    > 仅在 JSON 数据存在根节点时涉及，需要通过 `json_root` 参数来指定根节点。

3. 根据 `jsonpaths` 参数提取待导入的 JSON 数据。

##### **不指定 JSON 根节点、使匹配模式导入数据**

假设数据文件 `example2.json` 包含如下数据：

```JSON
[
    {"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},
    {"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},
    {"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
]
```

您可以通过指定 `jsonpaths` 参数进行精准导入，例如只导入 `category`、`author`、`price` 三个字段的数据：

```Bash
curl --location-trusted -u <username>:<password> -H "label:label7" \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **说明**
>
> 上述示例中，JSON 数据的最外层是一个通过中括号 [] 表示的数组结构，并且数组结构中的每个 JSON 对象都表示一条数据记录。因此，需要设置 `strip_outer_array` 为 `true`来裁剪最外层的数组结构。导入过程中，未指定的字段 `title` 会被忽略掉。

##### **指定 JSON 根节点、使匹配模式导入数据**

假设数据文件 `example3.json` 包含如下数据：

```JSON
{
    "id": 10001,
    "RECORDS":[
        {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
        {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
        {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
    ],
    "comments": ["3 records", "there will be 3 rows"]
}
```

您可以通过指定 `jsonpaths` 进行精准导入，例如只导入 `category`、`author`、`price` 三个字段的数据：

```Bash
curl --location-trusted -u <username>:<password> \
    -H "Expect:100-continue" \
    -H "format: json" \
    -H "json_root: $.RECORDS" \
    -H "strip_outer_array: true" \
    -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" \
    -H "columns: category, price, author" -H "label:label8" \
    -T example3.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/tbl1/_stream_load
```

> **说明**
>
> 上述示例中，JSON 数据的最外层是一个通过中括号 [] 表示的数组结构，并且数组结构中的每个 JSON 对象都表示一条数据记录。因此，需要设置 `strip_outer_array` 为 `true`来裁剪最外层的数组结构。导入过程中，未指定的字段 `title` 和 `timestamp` 会被忽略掉。另外，示例中还通过 `json_root` 参数指定了需要真正导入的数据为 `RECORDS` 字段对应的值，即一个 JSON 数组。
