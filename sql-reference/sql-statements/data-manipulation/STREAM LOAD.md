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

### 参数解析

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

```bash
curl "url"
```

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
