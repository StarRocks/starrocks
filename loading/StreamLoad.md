# Stream load

Stream Load 是一种同步的导入方式，您可以通过发送 HTTP 请求将本地文件或数据流导入到 StarRocks 中。Stream Load 会同步执行导入并返回导入结果。您可以直接通过请求的返回值判断导入是否成功。

为了支持和其他系统（如 Apache Flink® 和 Apache Kafka®）之间实现跨系统的两阶段提交，并提升高并发 Stream Load 导入场景下的性能，StarRocks 提供了 Stream Load 事务接口。您可以选择使用事务接口来执行 Stream Load 导入。请参阅 [使用 Stream Load 事务接口导入](/loading/Stream_Load_Transaction_Interface.md)。

## 支持的数据格式

StarRocks 支持 CSV 和 JSON 格式。导入 CSV 格式的数据时，单次导入的 CSV 文件的大小不能超过 10 GB；导入 JSON 格式的数据时，单次导入的 JSON 文件的大小不能超过 4 GB。

## 基本操作

### 创建导入任务

Stream Load 通过 HTTP 协议提交和传输数据。这里通过 `curl` 命令展示如何提交导入，您也可以通过其他 HTTP 客户端进行操作。示例如下：

```Bash
curl --location-trusted -u root -T date -H "label:123"  -H "columns: k1, k2, v1" -T testData \ http://abc.com:8030/api/test/date/_stream_load
```

> 说明：

- > 当前支持 HTTP 分块上传和非分块上传两种方式。对于非分块方式，必须要用 `Content-Length` 字段来标示上传内容的长度，从而保证数据完整性。

- > 最好设置 Expect HTTP 请求头中的 `100-continue` 字段，这样可以在某些出错的场景下避免不必要的数据传输。

签名参数说明如下：

`user` 和 `passwd`：Stream Load 创建导入任务使用的是 HTTP 协议，可通过基本认证 (Basic Access Authentication) 进行签名。StarRocks 系统会根据签名来验证登录用户的身份和导入权限。

导入任务参数说明如下：

Stream Load 中所有与导入任务相关的参数均设置在请求头中。下面介绍上述命令示例中部分参数的意义：

- `label`：导入任务的标签，相同标签的数据无法多次导入。您可以通过指定标签的方式来避免一份数据重复导入的问题。当前 StarRocks 系统会保留最近 30 分钟内已经成功完成的导入任务的标签。

- `columns`：用于指定要导入的源文件中的列与 StarRocks 数据表中的列之间的对应关系。如果源文件中的列正好对应表结构 (Schema)，则无需指定该参数。如果源文件与表结构不对应，则需要通过该参数来配置数据转换规则。

支持两种形式的列：一种是直接对应源文件中的字段，可直接使用字段名表示；一种需要通过计算得出。举几个例子帮助理解：

- 例 1：表中有 3 列，分别为 `c1`、`c2` 和 `c3`，依次对应源文件的 3 列 `c3`、`c2` 和 `c1`。这种情况下，需要指定 `-H "columns: c3, c2, c1"`。

- 例 2：表中有 3 列，分别为 `c1`、`c2` 和 `c3` ，与源文件的前 3 列 `c1`、`c2` 和 `c3` 一一对应，但是源文件中还余 1 列。这种情况下，需要指定 `-H "columns: c1, c2, c3, temp"`，其中，最后余的 1 列可随意指定名称（比如 `temp`）用于占位即可。

- 例 3：表中有 3 列，分别为 `year`、`month` 和 `day`，源文件只有一个时间列，为 `2018-06-01 01:02:03` 格式。这种情况下，可以指定 `-H "columns: col, year = year(col), month=month(col), day=day(col)"`。

返回结果如下：

导入完成后，Stream Load 会以 JSON 格式返回本次导入的相关内容，示例如下：

```JSON
{

    "TxnId": 1003,

    "Label": "b6f3bc78-0d2c-45d9-9e4c-faa0a0149bee",

    "Status": "Success",

    "ExistingJobStatus": "FINISHED", // optional

    "Message": "OK",

    "NumberTotalRows": 1000000,

    "NumberLoadedRows": 999999,

    "NumberFilteredRows": 1,

    "NumberUnselectedRows": 0,

    "LoadBytes": 40888898,

    "LoadTimeMs": 2144,

    "ErrorURL": "[http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005](http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005)"

}
```

返回结果中包括如下参数：

- `TxnId`：导入任务的事务 ID。用户可不感知。

- `Status`：导入任务最后的状态。

  - `Success`：表示导入任务成功，数据已经可见。

  - `Publish Timeout`：表示导入事务已经成功提交，但是由于某种原因数据并不能立即可见。可以视作已经成功不必重试导入。

  - `Label Already Exists`：表示该标签已经被其他导入任务占用，可能是导入成功，也可能是正在导入。

  - `Fail`：表示导入任务失败，您可以指定标签重试此次导入任务。

- `ExistingJobStatus`：当前导入作业的状态。

  - `RUNNING`：作业运行中。
  
  - `FINISHED`：作业已完成。

- `Message`：导入任务状态的详细说明。导入任务失败时会返回具体的失败原因。

- `NumberTotalRows`：从数据流中读取到的总行数。

- `NumberLoadedRows`：此次导入的数据行数。只有在返回结果中的 `Status` 为 `Success` 时有效。

- `NumberFilteredRows`：此次导入，因数据质量不合格而过滤掉的行数。

- `NumberUnselectedRows`：此次导入，通过 WHERE 条件被过滤掉的行数。

- `LoadBytes`：此次导入的源文件数据量大小。

- `LoadTimeMs`：此次导入所用的时间。单位是 ms。

- `ErrorURL`：被过滤数据的具体内容，仅保留前 1000 条。如果导入任务失败，可以用以下方式获取被过滤的数据，并进行分析，以调整导入任务。

```Bash
wget http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005
```

### 取消导入任务

不支持手动取消 Stream Load 任务。如果 Stream Load 任务发生超时或者导入错误，StarRocks 会自动取消该任务。

## JSON 数据导入

对于文本文件存储的 JSON 数据，可以采用 Stream Load 的方式进行导入。

样例数据：

```JSON
{ "id": 123, "city" : "beijing"},

{ "id": 456, "city" : "shanghai"},

    ...
```

导入示例：

```Bash
curl -v --location-trusted -u root:\

    -H "format: json" -H "jsonpaths: [\"$.id\", \"$.city\"]" \

    -T example.json \

    http://FE_HOST:HTTP_PORT/api/DATABASE/TABLE/_stream_load
curl --location-trusted -u root -H "strict_mode: true" \

-H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData \

http://host:port/api/testDb/testTbl/_stream_load
```

参数说明如下：

- `format`：指定要导入的数据的格式。这里设置为 `json`。

- `jsonpaths`：选择每一列的 JSON 路径。

- `json_root`：选择 JSON 开始解析的列。

- `strip_outer_array`：裁剪最外面的 array 字段。

- `strict_mode`：导入过程中的列类型转换执行严格过滤。

- `columns`：对应 StarRocks 表中的字段的名称。

`jsonpaths` 参数和 `columns` 参数还有 StarRocks 表中字段三者关系如下：

- `jsonpaths` 的值名称与 JSON 文件中的 key 的名称一致

- `columns` 的值的名称和 StarRocks 表中字段名称保持一致

- `columns` 和 `jsonpaths` 属性的值，名称不需要保持一致，但是建议设置为一致方便区分。值的顺序保持一致就可以将 JSON 文件中的值和 StarRocks 表中字段对应起来，如下图所示：

![img](https://starrocks.feishu.cn/space/api/box/stream/download/asynccode/?code=ODgzOWY0ODg0YWRlNmU5OGJlMzg5YzZlNTQzYjE0MGZfTmJEajZSOHdHcnhrbFNCUXM1cnhtcjlCOERaOEZrdHNfVG9rZW46Ym94Y25YTVBzbzFHRVFOSWczTlZMTzFUdnBmXzE2NTU3MzAwNzU6MTY1NTczMzY3NV9WNA)

样例数据：

```JSON
{"name": "北京", "code": 2}
```

导入示例：

```Bash
curl -v --location-trusted -u root: \

    -H "format: json" -H "jsonpaths: [\"$.name\", \"$.code\"]" \

    -H "columns: city,tmp_id, id = tmp_id * 100" \

    -T jsontest.json \

    http://127.0.0.1:8030/api/test/testJson/_stream_load
```

导入后结果：

```Plain%20Text
+------+------+

|  id  | city |

+------+------+

|  200 | 北京 |

+------+------+
```

如果想先对 JSON 中数据进行加工，然后再落入 StarRocks 表中，可以通过更改 `columns` 的值来实现，属性的对应关系可以参考上图中描述，示例如下:

样例数据：

```JSON
{"k1": 1, "k2": 2}
```

导入示例：

```Bash
curl -v --location-trusted -u root: \

    -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" \

    -H "columns: k2, tmp_k1, k1 = tmp_k1 * 100" \

    -T example.json \

    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

这里导入过程中进行了将 `k1` 乘以 100 的 ETL 操作，并且通过 `jsonpaths` 来进行 `columns` 和原始数据的对应。

导入后结果：

```Plain%20Text
+------+------+

|  k1  |  k2  |

+------+------+

|  100 |  2   |

+------+------+
```

对于缺失的列，如果列的定义是 `nullable`，那么会补上 `NULL`，也可以通过 `ifnull()` 函数补充默认值。

样例数据：

```JSON
[

    {"k1": 1, "k2": "a"},

    {"k1": 2},

    {"k1": 3, "k2": "c"},

]
```

> 这里最外层有一对表示 JSON 数组 的中括号 (`[ ]`)，导入时就需要指定 `strip_outer_array = true`。

导入示例 1：

```Bash
curl -v --location-trusted -u root: \

    -H "format: json" -H "strip_outer_array: true" \

    -T example.json \

    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

导入后结果：

```Plain%20Text
+------+------+

|  k1  | k2   |

+------+------+

|   1  | a    |

+------+------+

|   2  | NULL |

+------+------+

|   3  | c    |

+------+------+
```

导入示例 2：

```Bash
curl -v --location-trusted -u root: \

    -H "format: json" -H "strip_outer_array: true" \

    -H "jsonpaths: [\"$.k1\", \"$.k2\"]" \

    -H "columns: k1, tmp_k2, k2 = ifnull(tmp_k2, 'x')" \

    -T example.json \

    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

导入后结果：

```Plain%20Text
+------+------+

|  k1  |  k2  |

+------+------+

|  1   |  a   |

+------+------+

|  2   |  x   |

+------+------+

|  3   |  c   |

+------+------+
```

## 代码集成

- Java 集成 Stream Load，参考 [https://github.com/StarRocks/demo/MiscDemo/stream_load](https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load)。

- Apache Spark™ 集成 Stream Load，参考 [01_sparkStreaming2StarRocks](https://github.com/StarRocks/demo/blob/master/docs/01_sparkStreaming2StarRocks.md)。

## 最佳实践

请参考 [STREAM LOAD](/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md)。

## 常见问题

1. 数据质量问题报错 "ETL-QUALITY-UNSATISFIED; msg:quality not good enough to cancel" 应该怎么解决？

请参考[导入总览](/loading/Loading_intro.md)下的“常见问题”小节。

1. 导入状态为 "Label Already Exists" 应该怎么解决？

请参考[导入总览](/loading/Loading_intro.md)下的“常见问题”小节。由于 Stream Load 是采用 HTTP 协议提交创建导入任务的请求，一般各个语言的 HTTP 客户端均会自带请求重试逻辑。StarRocks 系统在接受到第一个请求后，已经开始操作 Stream Load，但是由于没有及时向客户端返回结果，客户端会发生再次重试创建请求的情况。这时候 StarRocks 系统由于已经在操作第一个请求，所以第二个请求会返回 `Label Already Exists` 的状态提示。排查上述可能的方法：使用标签搜索主 FE 的日志，看是否存在同一个标签出现了两次的情况。如果有就说明客户端重复提交了该请求。

建议根据当前请求的数据量，计算出大致的导入耗时，并根据导入超时时间改大客户端的请求超时时间，避免请求被客户端多次提交。
