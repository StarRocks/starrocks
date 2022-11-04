# STREAM LOAD

## 功能

Stream Load 用于向指定的 table 导入数据，与普通 Load 区别是，这种导入方式是 **同步导入**。用户通过发送 HTTP 请求将本地文件或数据流导入到 StarRocks 中, Stream load 能够保证一批导入任务的原子性，要么全部数据导入成功，要么全部失败。用户可直接通过请求的返回值判断导入是否成功。Stream load 的导入原理及适用场景请参考 [stream load](/loading/StreamLoad.md) 章节。

该操作会同时更新和此 base table 相关的 rollup table 的数据。

当前支持 HTTP chunked 与非 chunked 上传两种方式，对于非 chunked 方式，必须要有 Content-Length 来标示上传内容长度，这样能够保证数据的完整性。
另外，用户最好设置 Expect Header 字段内容 100-continue，这样可以在某些出错场景下避免不必要的数据传输。

## 语法

注：方括号 [] 中内容可省略不写。

```bash
curl --location-trusted -u user:passwd [-H ""...] -T data.file -XPUT \
http://fe_host:http_port/api/{db}/{table}/_stream_load
```

用户可以通过 HTTP 的 Header 部分来传入导入参数。

- 当前支持 HTTP **分块上传**和**非分块上传**两种方式。如果使用非分块上传方式，必须使用请求头字段 `Content-Length` 来标示待上传内容的长度，从而保证数据完整性。

  > 说明：使用 curl 工具提交导入作业的时候，会自动添加 `Content-Length` 字段，因此无需手动指定 `Content-Length`。

- 建议在 HTTP 请求的请求头字段 `Expect` 中指定 `100-continue`，即 `"Expect:100-continue"`。这样在服务器拒绝导入作业请求的情况下，可以避免不必要的数据传输，从而减少不必要的资源开销。

**label:**

一次导入的标签，相同标签的数据无法多次导入。用户可以通过指定 Label 的方式来避免一份数据重复导入的问题。
当前 StarRocks 内部保留 30 分钟内最近成功的 label。

**column_separator：**

用于指定导入文件中的列分隔符，默认为\t。如果是不可见字符，则需要加\x 作为前缀，使用十六进制来表示分隔符。
如 hive 文件的分隔符\x01，需要指定为 `-H "column_separator:\x01"`

**columns：**

用于指定导入文件中的列和 table 中的列的对应关系。如果源文件中的列正好对应表中的内容，那么是不需要指定这个字段的内容的。
如果源文件与表 schema 不对应，那么需要这个字段进行一些数据转换。这里有两种形式 column:

```plain text
1.直接对应导入文件中的字段，直接使用字段名表示；
2.衍生列，语法为 `column_name = expression`。
```

例如：

例 1: 表中有 3 个列“c1, c2, c3”，源文件中的三个列一次对应的是 "c3, c2, c1"; 那么需要指定-H "columns: c3, c2, c1"

例 2: 表中有 3 个列“c1, c2, c3 ", 源文件中前三列依次对应，但是有多余 1 列；那么需要指定 `-H "columns: c1, c2, c3, xxx"`; 最后一个列随意指定个名称占位即可。

例 3: 表中有 3 个列“year, month, day " 三个列，源文件中只有一个时间列，为”2018-06-01 01: 02: 03“格式；
那么可以指定 `-H "columns: col, year = year(col), month=month(col), day=day(col)"` 完成导入。

**where:**

用于抽取部分数据。用户如果有需要将不需要的数据过滤掉，那么可以通过设定这个选项来达到。
例如: 只导入大于 k1 列等于 20180601 的数据，那么可以在导入时候指定 `-H "where: k1 = 20180601"`。

**max_filter_ratio：**

最大容忍可过滤（数据不规范等原因）的数据比例。默认零容忍。数据不规范不包括通过 where 条件过滤掉的行。

**partitions:**

用于指定这次导入所设计的 partition。如果用户能够确定数据对应的 partition，推荐指定该项。不满足这些分区的数据将被过滤掉。
比如指定导入到 p1, p2 分区，`-H "partitions: p1, p2"`。

**timeout:**

指定导入的超时时间。单位秒。默认是 600 秒。可设置范围为 1 秒 ~ 259200 秒。

**strict_mode:**

用户指定此次导入是否开启严格模式，默认为关闭。开启方式为 `-H "strict_mode: true"`。

**timezone:**

指定本次导入所使用的时区。默认为东八区。该参数会影响所有导入涉及的和时区有关的函数结果。

**exec_mem_limit:**

导入内存限制。默认为 2GB。单位为字节。

**format:**

指定导入数据格式，默认是 csv，支持 json 格式。

**jsonpaths:**

参数值应为 JSON 格式。
导入 json 方式分为：简单模式和精准模式。
简单模式：没有设置 jsonpaths 参数即为简单模式，这种模式下要求 json 数据是对象类型，例如：
{"k1": 1, "k2": 2, "k3": "hello"}，其中 k1，k2，k3 是列名字。

匹配模式：用于 json 数据相对复杂，需要通过 jsonpaths 参数匹配对应的 value。

**strip_outer_array:**

布尔类型，为 true 表示 json 数据以数组对象开始且将数组对象中进行展平，默认值是 false。例如：
[
{"k1" : 1, "v1" : 2},
{"k1" : 3, "v1" : 4}
]
当 strip_outer_array 为 true，最后导入到 starrocks 中会生成两行数据。

**json_root:**

json_root 为合法的 jsonpath 字符串，用于指定 json document 的根节点，默认值为 ""。

### 返回值

导入完成后，会以 Json 格式返回这次导入的相关内容。当前包括一下字段
**Status:** 导入最后的状态。

**Success：** 表示导入成功，数据已经可见；

**Publish Timeout：** 表述导入作业已经成功 Commit，但是由于某种原因并不能立即可见。用户可以视作已经成功不必重试导入。
**Label Already Exists:** 表明该 Label 已经被其他作业占用，可能是导入成功，也可能是正在导入。
用户需要通过 `get label state` 命令来确定后续的操作。

**其他：** 此次导入失败，用户可以指定 Label 重试此次作业
Message: 导入状态详细的说明。失败时会返回具体的失败原因。

**NumberTotalRows:** 从数据流中读取到的总行数
**NumberLoadedRows:** 此次导入的数据行数，只有在 Success 时有效。
**NumberFilteredRows:** 此次导入过滤掉的行数，即数据质量不合格的行数。
**NumberUnselectedRows:** 此次导入，通过 where 条件被过滤掉的行数。
**LoadBytes:** 此次导入的源文件数据量大小。
**LoadTimeMs:** 此次导入所用的时间。
**ErrorURL:** 被过滤数据的具体内容，仅保留前 1000 条。

### 错误信息

可以通过以下语句查看导入错误详细信息：

```SQL
SHOW LOAD WARNINGS ON 'url'
```

其中 url 为 ErrorURL 给出的 url，通过以下命令可以查看错误详细信息：

#### 公共参数

| **参数名称** | **是否必选** | **参数说明**                                                 |
| ------------ | ------------ | ------------------------------------------------------------ |
| file_name    | 是           | 指定待导入数据文件的名称。文件名里可选包含或者不包含扩展名。 |
| format       | 否           | 指定待导入数据的格式。取值包括 `CSV` 和 `JSON`。默认值：`CSV`。 |
| partitions   | 否           | 指定要把数据导入哪些分区。如果不指定该参数，则默认导入到 StarRocks 表所在的所有分区中。 |
| columns      | 否           | 指定待导入数据文件和 StarRocks 表之间的列对应关系。如果待导入数据文件中的列与 StarRocks 表中的列按顺序一一对应，则不需要指定 `columns` 参数。您可以通过 `columns` 参数实现数据转换。例如，要导入一个 CSV 格式的数据文件，文件中有两列，分别可以对应到目标 StarRocks 表的 `id` 和 `city` 两列。如果要实现把数据文件中第一列的数据乘以 100 以后再落入 StarRocks 表的转换，可以指定 `"columns: city,tmp_id, id = tmp_id * 100"`。具体请参见本文“[列映射](/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md#列映射)”章节。 |

#### CSV 适用参数

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| column_separator | 否           | 用于指定待导入数据文件中的列分隔符。如果不指定该参数，则默认为 `\t`，即 Tab。必须确保这里指定的列分隔符与待导入数据文件中的列分隔符一致。<br>说明：StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (\|)。 |
| row_delimiter    | 否           | 用于指定待导入数据文件中的行分隔符。如果不指定该参数，则默认为 `\n`。 |

#### JSON 适用参数

| **参数名称**      | **是否必选** | **参数说明**                                                 |
| ----------------- | ------------ | ------------------------------------------------------------ |
| jsonpaths         | 否           | 用于指定待导入的字段的名称。参数值应为 JSON 格式。Stream Load 支持通过如下模式之一来导入 JSON 格式的数据：简单模式和匹配模式。 该参数仅用于通过匹配模式导入 JSON 格式的数据。<ul><li>简单模式：不需要设置 `jsonpaths` 参数。这种模式下，要求 JSON 数据是对象类型，例如 `{"k1": 1, "k2": 2, "k3": "hello"}` 中，`k1`、`k2`、`k3` 是字段的名称，按名称直接对应目标 StarRocks 表中的`col1`、`col2`、`col3` 三列。</li><li>匹配模式：用于 JSON 数据相对复杂、需要通过 `jsonpaths` 参数匹配待导入字段的场景。</li></ul>具体请参见本文提供的示例[使用匹配模式导入数据](/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md#使用匹配模式导入数据)。 |
| strip_outer_array | 否           | 用于指定是否裁剪最外面的 `array` 含义。取值范围：`true` 和 `false`。默认值：`false`。`true` 表示 JSON 格式文件中的数据是以数组形式表示的。如果待导入数据文件中最外层有一对表示 JSON 数组的中括号 (`[]`)，则一般情况下需要指定该参数取值为 `true`，这样中括号 (`[]`) 中每一个数组元素都作为单独的一行数据行进行导入；否则，StarRocks 会将整个文件数据（即，整个 JSON 数组）作为一行数据导入。例如，JSON 格式的数据为 `[ {"k1" : 1, "v1" : 2}, {"k1" : 3, "v1" : 4} ]`，如果指定该参数取值为 `true`，则导入到 StarRocks 表中后会生成两行数据。 |
| json_root         | 否           | 用于指定待导入 JSON 数据的根节点。该参数仅用于通过匹配模式导入 JSON 格式的数据。`json_root` 为合法的 JsonPath 字符串。默认值为空，表示会导入整个导入文件的数据。具体请参见本文提供的示例[导入数据并指定 JSON 根节点](/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md#导入数据并指定 JSON 根节点)。 |
| ignore_json_size | 否   | 用于指定是否检查 HTTP 请求中 JSON Body 的大小。<br/>**说明**<br/>HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。 |

另外，导入 JSON 格式的数据时，需要注意单个 JSON 对象的大小不能超过 4 GB。如果 JSON 文件中单个 JSON 对象的大小超过 4 GB，会提示 "This parser can't support a document that big." 错误。

### `opt_properties`

用于指定一些导入相关的可选参数。指定的参数设置作用于整个导入作业。语法如下：

```Bash
-H "label: <label_name>"
-H "where: <condition1>[, <condition2>, ...]"
-H "max_filter_ratio: <num>"
-H "timeout: <num>"
-H "strict_mode: true | false"
-H "timezone: <string>"
-H "load_mem_limit: <num>"
```

参数说明如下表所述。

| **参数名称**     | **是否必选** | **参数说明**                                                 |
| ---------------- | ------------ | ------------------------------------------------------------ |
| label            | 否           | 用于指定导入作业的标签。如果您不指定标签，StarRocks 会自动为导入作业生成一个标签。相同标签的数据无法多次成功导入，这样可以避免一份数据重复导入。有关标签的命名规范，请参见[系统限制](/reference/System_limit.md)。StarRocks 默认保留最近 3 天内成功的导入作业的标签。您可以通过 [FE 配置参数](/administration/Configuration.md#导入和导出相关动态参数) `label_keep_max_second` 设置默认保留时长。 |
| where            | 否           | 用于指定过滤条件。如果指定该参数，StarRocks 会按照指定的过滤条件对转换后的数据进行过滤。只有符合 WHERE 子句中指定的过滤条件的数据才会导入。 |
| max_filter_ratio | 否           | 用于指定导入作业的最大容错率，即导入作业能够容忍的因数据质量不合格而过滤掉的数据行所占的最大比例。取值范围：`0`~`1`。默认值：`0` 。<br>建议您保留默认值 `0`。这样的话，当导入的数据行中有错误时，导入作业会失败，从而保证数据的正确性。<br>如果希望忽略错误的数据行，可以设置该参数的取值大于 `0`。这样的话，即使导入的数据行中有错误，导入作业也能成功。<br>说明：这里因数据质量不合格而过滤掉的数据行，不包括通过 WHERE 子句过滤掉的数据行。 |
| timeout          | 否           | 用于导入作业的超时时间。单位：秒。取值范围：1 ~ 259200。默认值：`600`。<br>说明：除了 `timeout` 参数可以控制该导入作业的超时时间外，您还可以通过 [FE 配置参数](/administration/Configuration.md#导入和导出相关动态参数) `stream_load_default_timeout_second` 来统一控制 Stream Load 导入作业的超时时间。如果指定了`timeout` 参数，则该导入作业的超时时间以 `timeout` 参数为准；如果没有指定 `timeout` 参数，则该导入作业的超时时间以`stream_load_default_timeout_second` 为准。 |
| strict_mode      | 否           | 用于指定是否开启严格模式。取值范围：`true` 和 `false`。默认值：`false`。`true` 表示开启，`false` 表示关闭。 |
| timezone         | 否           | 用于指定导入作业所使用的时区。默认为东八区 (Asia/Shanghai)。<br>该参数的取值会影响所有导入涉及的、跟时区设置有关的函数所返回的结果。受时区影响的函数有 strftime、alignment_timestamp 和 from_unixtime 等，具体请参见[设置时区](/administration/timezone.md)。导入参数 `timezone` 设置的时区对应“[设置时区](/administration/timezone.md)”中所述的会话级时区。 |
| load_mem_limit   | 否           | 导入作业的内存限制，最大不超过 BE 的内存限制。单位：字节。默认内存限制为 2 GB。 |

## 列映射

### 导入 CSV 数据时配置列映射关系

在导入 CSV 格式的数据时，只需要通过 `columns` 参数来指定待导入数据文件和 StarRocks 表之间的列映射关系。如果待导入数据文件中的列与 StarRocks 表中的列按顺序一一对应，则不需要指定该参数；否则，必须通过该参数来配置列映射关系，一般包括如下两种场景：

- 待导入数据文件中各个列的数据不需要通过函数计算、可以直接落入 StarRocks 表中对应的列。

  您需要在 `columns` 参数中按照待导入数据文件中的列顺序、使用 StarRocks 表中对应的列名来配置列映射关系。

  例如，StarRocks 表中有三列，按顺序依次为 `col1`、`col2` 和 `col3`；待导入数据文件中也有三列，按顺序依次对应 StarRocks 表中的 `col3`、`col2` 和 `col1`。这种情况下，需要指定 `"columns: col3, col2, col1"`。

- 待导入数据文件中某些列的数据需要通过函数计算以后才能落入 StarRocks 表中对应的列。

  您不仅需要在 `columns` 参数中按照待导入数据文件中的列顺序、使用 StarRocks 表中对应的列名来配置列映射关系，还需要指定参与数据计算的函数。以下为两个示例：

  - StarRocks 表中有三列，按顺序依次为 `col1`、`col2` 和 `col3` ；待导入数据文件中有四列，前三列按顺序依次对应 StarRocks 表中的 `col1`、`col2` 和 `col3`，第四列在 StarRocks 表中无对应的列。这种情况下，需要指定 `"columns: col1, col2, col3, temp"`，其中，最后一列可随意指定一个名称（如 `temp`）用于占位即可。
  - StarRocks 表中有三列，按顺序依次为 `year`、`month` 和 `day`。待导入数据文件中只有一个包含时间数据的列，格式为 `yyyy-mm-dd hh:mm:ss`。这种情况下，可以指定 `"columns: col, year = year(col), month=month(col), ``day=day(col)``"`。其中，`col` 是待导入数据文件中所包含的列的临时命名，`year = year(col)`、`month=month(col)` 和 `day=day(col)` 用于指定从待导入数据文件中的 `col` 列提取对应的数据并落入 StarRocks 表中对应的列，如 `year = year(col)` 表示通过 `year` 函数提取待导入数据文件中 `col` 列的 `yyyy` 部分的数据并落入 StarRocks 表中的 `year` 列。

### 导入 JSON 数据时配置列映射关系

在导入 JSON 格式的数据时，需要通过 `jsonpaths` 和 `columns` 两个参数来指定待导入数据文件和 StarRocks 表之间的列映射关系：

- `jsonpaths` 参数中声明的字段与待导入数据文件中的字段**按名称**保持一一对应。

- `columns` 中声明的列与 `jsonpaths` 中声明的字段**按顺序**保持一一对应。

- `columns` 参数中声明的列与 StarRocks 表中的列**按名称**保持一一对应。

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
    BeginTxnTimeMs: 0,
    StreamLoadPutTimeMS: 1,
    ReadDataTimeMs: 0,
    WriteDataTimeMs: 11,
    CommitAndPublishTimeMs: 16,
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
| StreamLoadPutTimeMS    | 导入作业生成执行计划的时长。                                 |
| ReadDataTimeMs         | 导入作业读取数据的时长。                                     |
| WriteDataTimeMs        | 导入作业写入数据的时长。                                     |
| CommitAndPublishTimeMs | 导入作业提交和数据发布的耗时。                               |

如果导入作业失败，还会返回 `ErrorURL`，如下所示：

```JSON
{
    "ErrorURL": "http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be"
}
```

通过 `ErrorURL` 可以查看导入过程中因数据质量不合格而过滤掉的错误数据行的具体信息，当前仅保留前 1000 条。

您可以通过  `curl "url"` 命令直接查看错误数据行的信息。也可以通过 `wget "url"` 命令导出错误数据行的信息，如下所示：

```Bash
wget http://172.26.195.68:8045/api/_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be
```

导出的错误数据行信息会保存到一个名为 `_load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` 的本地文件中。您可以通过 `cat _load_error_log?file=error_log_3a4eb8421f0878a6_9a54df29fd9206be` 命令查看该文件的内容。

您可以根据错误信息调整导入作业，然后重新提交导入作业。

## 示例

### 将本地文件导入 Starrocks 表中

将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用 Label 用于去重。指定超时时间为 100 秒。

```bash
curl --location-trusted -u root -H "label:123" -H "timeout:100" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 对导入数据进行条件筛选

将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表，使用 Label 用于去重, 并且只导入 k1 等于 20180601 的数据。

```bash
curl --location-trusted -u root -H "label:123" -H "where: k1=20180601" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 设置导入任务允许错误率

将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许 20%的错误率（用户是 defalut_cluster 中的）。

```bash
curl --location-trusted -u root -H "label:123" -H "max_filter_ratio:0.2" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 导入数据时指定列名

将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表, 允许 20%的错误率，并且指定文件的列名（用户是 defalut_cluster 中的）。

```bash
curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" \
-H "columns: k2, k1, v1" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 将数据导入对应分区

将本地文件'testData'中的数据导入到数据库'testDb'中'testTbl'的表中的 p1, p2 分区, 允许 20%的错误率。

```bash
curl --location-trusted -u root  -H "label:123" -H "max_filter_ratio:0.2" \
-H "partitions: p1, p2" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 试用 Streaming 方式导入

使用 streaming 方式导入（用户是 defalut_cluster 中的）。

```sql
seq 1 10 | awk '{OFS="\t"}{print $1, $1 * 10}' | curl --location-trusted -u root -T - \
http://host:port/api/testDb/testTbl/_stream_load
```

### 导入数据到含有 HLL 列的表中

导入含有 HLL 列的表，可以是表中的列或者数据中的列用于生成 HLL 列，也可使用 hll_empty 补充数据中没有的列。

```bash
curl --location-trusted -u root \
-H "columns: k1, k2, v1=hll_hash(k1), v2=hll_empty()" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 以严格模式导入数据并设置时区

导入数据进行严格模式过滤，并设置时区为 Africa/Abidjan。

```bash
curl --location-trusted -u root -H "strict_mode: true" \
-H "timezone: Africa/Abidjan" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 导入数据到含有 BITMAP 列的表中

导入含有 BITMAP 列的表，可以是表中的列或者数据中的列用于生成 BITMAP 列，也可以使用 bitmap_empty 填充空的 Bitmap

```bash
curl --location-trusted -u root \
-H "columns: k1, k2, v1=to_bitmap(k1), v2=bitmap_empty()" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

### 导入 Json 格式数据

简单模式，导入 json 数据

json 数据格式：

```plain text
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

```sql
-- 表结构：
`category` varchar(512) NULL COMMENT "",
`author` varchar(512) NULL COMMENT "",
`title` varchar(512) NULL COMMENT "",
`price` double NULL COMMENT ""
```

导入命令：

```bash
curl --location-trusted -u root  -H "label:123" -H "format: json" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

为了提升吞吐量，支持一次性导入多条数据，json 数据格式如下：

```plain text
[
{"category":"C++","author":"avc","title":"C++ primer","price":89.5},
{"category":"Java","author":"avc","title":"Effective Java","price":95},
{"category":"Linux","author":"avc","title":"Linux kernel","price":195}
]
```

### 以匹配模式导入 Json 格式数据

```plain text
json数据格式：
[
{"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},
{"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},
{"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
]
```

通过指定 `jsonpath` 进行精准导入，例如只导入 category、author、price 三个属性:

```bash
curl --location-trusted -u root \
-H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

说明：
1）如果 json 数据是以数组开始，并且数组中每个对象是一条记录，则需要将 `strip_outer_array` 设置成 true，表示展平数组。
2）如果 json 数据是以数组开始，并且数组中每个对象是一条记录，在设置 `jsonpath` 时，我们的 ROOT 节点实际上是数组中对象。

### 导入数据并指定 json 根节点

```plain text
json数据格式:
{
"RECORDS":[
{"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
{"category":"22","author":"2avc","price":895,"timestamp":1589191487},
{"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
]
}
```

通过指定 `jsonpath` 进行精准导入，例如只导入 category、author、price 三个属性:

```bash
curl --location-trusted -u root \
-H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData \
http://host:port/api/testDb/testTbl/_stream_load
```

## 关键字(keywords)

STREAM, LOAD
