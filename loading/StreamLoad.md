# Stream load

Stream Load 是一种同步的导入方式，需要通过发送 HTTP 请求将本地文件或数据流导入到 StarRocks 中。StarRocks 会同步执行导入并返回导入的结果状态。您可以通过导入的结果状态来判断导入是否成功。

Stream Load 适用于导入数据量小于 10 GB 的本地文件、或通过程序导入数据流的业务场景。

## 支持的数据格式

StarRocks 支持如下数据格式：

- CSV

  导入 CSV 格式的数据时，单次导入的 CSV 文件的大小不能超过 10 GB。

- JSON

  导入 JSON 格式的数据时，单次导入的 JSON 文件的大小不能超过 4 GB。

## 基本操作

### 创建导入作业

这里通过 `curl` 命令展示如何提交导入作业，您也可以通过其他 HTTP 客户端进行操作。有关更多示例和参数说明，请参见 [STREAM LOAD](/sql-reference/sql-statements/data-manipulation/STREAM%20LOAD.md)。

#### 命令示例

```Bash
curl --location-trusted -u root -H "label:123"  -H "columns: k1, k2, v1" -T testData  http://abc.com:8030/api/test/date/_stream_load
```

> 说明：

- > 当前支持 HTTP 分块上传和非分块上传两种方式。对于非分块方式，必须要用 `Content-Length` 字段来标示上传内容的长度，从而保证数据完整性。

- > 最好设置 Expect HTTP 请求头中的 `100-continue` 字段，这样可以在某些出错的场景下避免不必要的数据传输。

#### 参数说明

`user` 和 `passwd` 是用于登录的用户名和密码。Stream Load 创建导入作业使用的是 HTTP 协议，可通过基本认证 (Basic Access Authentication) 进行签名。StarRocks 系统会根据签名来验证登录用户的身份和导入权限。

Stream Load 中所有与导入作业相关的参数均设置在请求头中。下面介绍上述命令示例中部分参数的含义：

- `label`：导入作业的标签，相同标签的数据无法多次导入。您可以通过指定标签的方式来避免一份数据重复导入的问题。当前 StarRocks 系统会保留最近 30 分钟内已经成功完成的导入作业的标签。

- `columns`：用于指定要导入的源文件（以下简称为“源文件”）中的字段与 StarRocks 数据表（以下简称为“目标表”）中的字段之间的对应关系。如果源文件中的字段正好对应目标表的表结构 (Schema)，则无需指定该参数。如果源文件与目标表的表结构不对应，则需要通过该参数来配置数据转换规则。

这里目标表中的字段分为两种情况：一种是直接对应源文件中的字段，这种情况下可以直接使用字段名表示；一种是不直接对应源文件中的字段，需要通过计算得出。举几个例子：

- 例 1：目标表中有 3 个字段，分别为 `c1`、`c2` 和 `c3`，依次对应源文件的 3 个字段 `c3`、`c2` 和 `c1`。这种情况下，需要指定 `-H "columns: c3, c2, c1"`。

- 例 2：目标表中有 3 个字段，分别为 `c1`、`c2` 和 `c3` ，与源文件的前 3 个字段 `c1`、`c2` 和 `c3` 一一对应，但是源文件中还余 1 个字段。这种情况下，需要指定 `-H "columns: c1, c2, c3, temp"`，其中，最后余的 1 个字段可随意指定一个名称（比如 `temp`）用于占位即可。

- 例 3：目标表中有 3 个字段，分别为 `year`、`month` 和 `day`。源文件只有一个时间字段，为 `2018-06-01 01:02:03` 格式。这种情况下，可以指定 `-H "columns: col, year = year(col), month=month(col), day=day(col)"`。

#### 返回结果

导入完成后，Stream Load 会以 JSON 格式返回本次导入作业的相关信息，示例如下：

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

- `TxnId`：导入作业的事务 ID。用户可不感知。
- `Status`：导入作业最后的状态。

  - `Success`：表示导入作业成功，数据已经可见。

  - `Publish Timeout`：表示导入事务已经成功提交，但是由于某种原因数据并不能立即可见。可以视作已经成功不必重试导入。

  - `Label Already Exists`：表示该标签已经被其他导入作业占用，可能是导入成功，也可能是正在导入。

  - `Fail`：表示导入作业失败，您可以指定标签重试此次导入作业。

- `ExistingJobStatus`：当前导入作业的状态。

  - `RUNNING`：作业运行中。

  - `FINISHED`：作业已完成。

- `Message`：导入作业状态的详细说明。导入作业失败时会返回具体的失败原因。

- `NumberTotalRows`：从数据流中读取到的总行数。

- `NumberLoadedRows`：此次导入的数据行数。只有在返回结果中的 `Status` 为 `Success` 时有效。

- `NumberFilteredRows`：此次导入，因数据质量不合格而过滤掉的行数。

- `NumberUnselectedRows`：此次导入，通过 WHERE 条件被过滤掉的行数。

- `LoadBytes`：此次导入的源文件数据量大小。

- `LoadTimeMs`：此次导入所用的时间。单位是 ms。

- `ErrorURL`：被过滤数据的具体内容，仅保留前 1000 条。

如果导入作业失败，可以使用 `wget` 命令获取过滤掉的数据，并进行分析，以调整导入作业。命令示例如下：

```Bash
wget http://192.168.1.1:8042/api/_load_error_log?file=__shard_0/error_log_insert_stmt_db18266d4d9b4ee5-abb00ddd64bdf005_db18266d4d9b4ee5_abb00ddd64bdf005
```

### 取消导入作业

不支持手动取消 Stream Load 作业。如果 Stream Load 作业发生超时或者导入错误，StarRocks 会自动取消该作业。

## 导入 JSON 数据

对于文本文件存储的 JSON 数据，可以采用 Stream Load 的方式进行导入。

### 命令示例

```Bash
curl --location-trusted -u root -H "strict_mode: true" \

-H "columns: category, price, author" -H "label:123" -H "format: json" -H "jsonpaths: [\"$.category\",\"$.price\",\"$.author\"]" -H "strip_outer_array: true" -H "json_root: $.RECORDS" -T testData \

http://host:port/api/testDb/testTbl/_stream_load
```

### 参数说明

- `format`：指定要导入的数据的格式。这里设置为 `json`。

- `jsonpaths`：选择每一个字段的 JSON 路径。

- `json_root`：选择 JSON 开始解析的字段。

- `strip_outer_array`：裁剪最外面的 `array` 字段。

- `strict_mode`：导入过程中对字段类型的转换执行严格过滤。

- `columns`：对应目标表中的字段的名称。

`jsonpaths` 参数、 `columns` 参数和目标表中的字段名称之间拥有如下关系：

- `jsonpaths` 中指定的字段名称与源文件中的字段名称保持一致。

- `columns` 中指定的字段名称和目标表中的字段名称保持一致。字段按名称匹配，与顺序无关。

- `columns` 中指定的字段名称和 `jsonpaths` 中指定的字段名称不需要保持一致。字段按顺序匹配，与名称无关。但是建议设置为一致，以方便区分。

字段之间的关系如下图所示：

![字段关系图](/assets/4.2.2-2.png)

如上图所示，源文件包含 `name` 和 `code` 两个字段，对应 `jsonpaths` 参数中的 `name` 和 `code`。 `jsonpaths` 参数中的 `name` 和 `code` 跟 `columns` 参数中的 `city` 和 `tmp_id` 按字段顺序匹配。`columns` 参数中的 `city` 和 `id` 跟目标表中的 `city` 和 `id` 按字段名称匹配。StarRocks 系统从源文件中获取 `name` 字段的取值，对应到目标表中的 `city` 字段；从源文件中获取 `code` 字段的取值，对应到 `tmp_id`，然后通过计算得到目标表中 `id` 字段的取值。

### 导入示例

样例数据：

```JSON
{"name": "北京", "code": 2}
```

执行导入：

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

如果想先对源文件中的数据进行加工，然后再落入目标表中，可以通过更改 `columns` 的取值来实现。

样例数据：

```JSON
{"k1": 1, "k2": 2}
```

执行导入：

```Bash
curl -v --location-trusted -u root: \

    -H "format: json" -H "jsonpaths: [\"$.k2\", \"$.k1\"]" \

    -H "columns: k2, tmp_k1, k1 = tmp_k1 * 100" \

    -T example.json \

    http://127.0.0.1:8030/api/db1/tbl1/_stream_load
```

这里导入过程中进行了将 `k1` 乘以 100 的 ETL 操作，并且通过 `jsonpaths` 来进行 `columns` 和源文件数据的对应。

导入后结果：

```Plain%20Text
+------+------+

|  k1  |  k2  |

+------+------+

|  100 |  2   |

+------+------+
```

对于缺失的字段，如果字段的定义是 `nullable`，那么会补上 `NULL`，也可以通过 `ifnull()` 函数补充默认值。

样例数据：

```JSON
[

    {"k1": 1, "k2": "a"},

    {"k1": 2},

    {"k1": 3, "k2": "c"},

]
```

> 说明：这里最外层有一对表示 JSON 数组的中括号 (`[]`)，导入时就需要指定 `strip_outer_array` 为 `true`。

执行导入示例 1：

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

执行导入示例 2：

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

- Java 集成 Stream Load，请参考 [https://github.com/StarRocks/demo/MiscDemo/stream_load](https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load)。

- Apache Spark™ 集成 Stream Load，请参考 [01_sparkStreaming2StarRocks](https://github.com/StarRocks/demo/blob/master/docs/01_sparkStreaming2StarRocks.md)。

## 常见问题

参见 [Stream Load 常见问题](/faq/loading/Stream_load_faq.md)。
