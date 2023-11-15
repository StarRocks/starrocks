# 通过 HTTP PUT 从本地文件系统或流式数据源导入数据

StarRocks 提供基于 HTTP 协议的 Stream Load 导入方式，帮助您从本地文件系统或流式数据源导入数据。

Stream Load 是一种同步的导入方式。您提交导入作业以后，StarRocks 会同步地执行导入作业，并返回导入作业的结果信息。您可以通过返回的结果信息来判断导入作业是否成功。

Stream Load 适用于以下业务场景：

- 导入本地数据文件。

  一般可采用 curl 命令直接提交一个导入作业，将本地数据文件的数据导入到 StarRocks 中。

- 导入实时产生的数据流。

  一般可采用 Apache Flink® 等程序提交一个导入作业，持续生成一系列导入任务，将实时产生的数据流持续不断地导入到 StarRocks 中。

另外，Stream Load 支持在导入过程中做数据的转换，具体请参见[导入过程中实现数据转换](../loading/Etl_in_loading.md)。

> 注意：Stream Load 操作会同时更新和 StarRocks 原始表相关的物化视图的数据。

## 支持的数据文件格式

Stream Load 支持如下数据文件格式：

- CSV
- JSON

您可以通过 `streaming_load_max_mb` 参数来设置单个待导入数据文件的大小上限，但一般不建议调大此参数。具体请参见本文档“[参数配置](../loading/StreamLoad.md#参数配置)”章节。

> **说明**
>
> 对于 CSV 格式的数据，StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。

## 使用限制

Stream Load 当前不支持导入某一列为 JSON 的 CSV 文件的数据。

## 基本原理

您需要在客户端上通过 HTTP 发送导入作业请求给 FE，FE 会通过 HTTP 重定向 (Redirect) 指令将请求转发给某一个 BE。或者，您也可以直接发送导入作业请求给某一个 BE。

> **说明**
>
> 如果把导入作业请求发送给 FE，FE 会通过轮询机制选定由哪一个 BE 来接收请求，从而实现 StarRocks 集群内的负载均衡。因此，推荐您把导入作业请求发送给 FE。

接收导入作业请求的 BE 作为 Coordinator BE，将数据按表结构划分、并分发数据到其他各相关的 BE。导入作业的结果信息由 Coordinator BE 返回给客户端。需要注意的是，如果您在导入过程中停止 Coordinator BE，会导致导入作业失败。

下图展示了 Stream Load 的主要流程：

![Stream Load 原理图](../assets/4.2-1-zh.png)

## 导入本地文件

### 创建导入作业

本文以 curl 工具为例，介绍如何使用 Stream Load 从本地文件系统导入 CSV 或 JSON 格式的数据。有关创建导入作业的详细语法和参数说明，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

#### 导入 CSV 格式的数据

##### 数据样例

1. 在数据库 `test_db` 中创建一张名为 `table1` 的主键模型表。表包含 `id`、`name` 和 `score` 三列，主键为 `id` 列，如下所示：

    ```SQL
    MySQL [test_db]> CREATE TABLE `table1`
    (
        `id` int(11) NOT NULL COMMENT "用户 ID",
        `name` varchar(65533) NULL COMMENT "用户姓名",
        `score` int(11) NOT NULL COMMENT "用户得分"
    )
    ENGINE=OLAP
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10;
    ```

2. 在本地文件系统中创建一个 CSV 格式的数据文件 `example1.csv`。文件一共包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

    ```Plain_Text
    1,Lily,23
    2,Rose,23
    3,Alice,24
    4,Julia,25
    ```

##### 命令示例

通过如下命令，把 `example1.csv` 文件中的数据导入到 `table1` 表中：

```Bash
curl --location-trusted -u root: -H "label:123" \
    -H "column_separator:," \
    -H "columns: id, name, score" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table1/_stream_load
```

`example1.csv` 文件中包含三列，跟 `table1` 表的 `id`、`name`、`score` 三列一一对应，并用逗号 (,) 作为列分隔符。因此，需要通过 `column_separator` 参数指定列分隔符为逗号 (,)，并且在 `columns` 参数中按顺序把 `example1.csv` 文件中的三列临时命名为 `id`、`name`、`score`。`columns` 参数中声明的三列，按名称对应 `table1` 表中的三列。

##### 查询数据

导入完成后，查询 `table1` 表的数据，如下所示：

```SQL
MySQL [test_db]> SELECT * FROM table1;

+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    1 | Lily  |    23 |
|    2 | Rose  |    23 |
|    3 | Alice |    24 |
|    4 | Julia |    25 |
+------+-------+-------+

4 rows in set (0.00 sec)
```

#### 导入 JSON 格式的数据

##### 数据样例

1. 在数据库 `test_db` 中创建一张名为 `table2` 的主键模型表。表包含 `id` 和 `city` 两列，主键为 `id` 列，如下所示：

    ```SQL
    MySQL [test_db]> CREATE TABLE `table2`
    (
        `id` int(11) NOT NULL COMMENT "城市 ID",
        `city` varchar(65533) NULL COMMENT "城市名称"
    )
    ENGINE=OLAP
    PRIMARY KEY(`id`)
    DISTRIBUTED BY HASH(`id`) BUCKETS 10;
    ```

2. 在本地文件系统中创建一个 JSON 格式的数据文件 `example2.json`。文件一共包含两个字段，分别代表城市名称和城市 ID，如下所示：

    ```JSON
    {"name": "北京", "code": 2}
    ```

##### 命令示例

通过如下语句把 `example2.json` 文件中的数据导入到 `table2` 表中：

```Bash
curl -v --location-trusted -u root: -H "strict_mode: true" \
    -H "format: json" -H "jsonpaths: [\"$.name\", \"$.code\"]" \
    -H "columns: city,tmp_id, id = tmp_id * 100" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/test_db/table2/_stream_load
```

`example2.json` 文件中包含 `name` 和 `code` 两个键，跟 `table2` 表中的列之间的对应关系如下图所示。

![JSON 映射图](../assets/4.2-2.png)

上图所示的对应关系描述如下：

- 提取 `example2.json` 文件中包含的 `name` 和 `code` 两个字段，按顺序依次映射到 `jsonpaths` 参数中声明的 `name` 和 `code` 两个字段。
- 提取 `jsonpaths` 参数中声明的 `name` 和 `code` 两个字段，**按顺序映射**到 `columns` 参数中声明的 `city` 和 `tmp_id` 两列。
- 提取 `columns` 参数声明中的 `city` 和 `id` 两列，**按名称映射**到 `table2` 表中的 `city` 和 `id` 两列。

> **说明**
>
> 上述示例中，在导入过程中先将 `example2.json` 文件中 `code` 字段对应的值乘以 100，然后再落入到 `table2` 表的 `id` 中。

有关导入 JSON 数据时 `jsonpaths`、`columns` 和 StarRocks 表中的字段之间的对应关系，请参见 STREAM LOAD 文档中“[列映射](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#列映射)”章节。

##### 查询数据

导入完成后，查询 `table2` 表的数据，如下所示：

```SQL
MySQL [test_db]> SELECT * FROM table2;
+------+--------+
| id   | city   |
+------+--------+
| 200  | 北京    |
+------+--------+
4 rows in set (0.01 sec)
```

### 查看导入作业

导入作业结束后，StarRocks 会以 JSON 格式返回本次导入作业的结果信息，具体请参见 STREAM LOAD 文档中“[返回值](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#返回值)”章节。

Stream Load 不支持通过 SHOW LOAD 语句查看导入作业执行情况。

### 取消导入作业

Stream Load 不支持手动取消导入作业。如果导入作业发生超时或者导入错误，StarRocks 会自动取消该作业。

## 导入数据流

Stream Load 支持通过程序导入数据流，具体操作方法，请参见如下文档：

- Flink 集成 Stream Load，请参见[使用 flink-connector-starrocks 导入至 StarRocks](../loading/Flink-connector-starrocks.md)。
- Java 集成 Stream Load，请参见 [https://github.com/StarRocks/demo/MiscDemo/stream_load](https://github.com/StarRocks/demo/tree/master/MiscDemo/stream_load)。
- Apache Spark™ 集成 Stream Load，请参见 [01_sparkStreaming2StarRocks](https://github.com/StarRocks/demo/blob/master/docs/01_sparkStreaming2StarRocks.md)。

## 参数配置

这里介绍使用 Stream Load 导入方式需要注意的一些系统参数配置。这些参数作用于所有 Stream Load 导入作业。

- `streaming_load_max_mb`：单个待导入数据文件的大小上限。默认文件大小上限为 10 GB。具体请参见 [BE 配置项](../administration/Configuration.md#be-配置项)。

  建议一次导入的数据量不要超过 10 GB。如果数据文件的大小超过 10 GB，建议您拆分成若干小于 10 GB 的文件分次导入。如果由于业务场景需要，无法拆分数据文件，可以适当调大该参数的取值，从而提高数据文件的大小上限。

  需要注意的是，如果您调大该参数的取值，需要重启 BE 才能生效，并且系统性能有可能会受影响，并且也会增加失败重试时的代价。

> **说明**
>
> 导入 JSON 格式的数据时，需要注意以下两点：
>
> - 单个 JSON 对象的大小不能超过 4 GB。如果 JSON 文件中单个 JSON 对象的大小超过 4 GB，会提示 "This parser can't support a document that big." 错误。
> - HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。

- `stream_load_default_timeout_second`：导入作业的超时时间。默认超时时间为 600 秒。具体请参见 [FE 动态参数](../administration/Configuration.md#配置-fe-动态参数)。

  如果您创建的导入作业经常发生超时，可以通过该参数适当地调大超时时间。您可以通过如下公式计算导入作业的超时时间：

  **导入作业的超时时间 > 待导入数据量/平均导入速度**

  例如，如果待导入数据文件的大小为 10 GB，并且当前 StarRocks 集群的平均导入速度为 100 MB/s，则超时时间应该设置为大于 100 秒。

  > **说明**
  >
  > “平均导入速度”是指目前 StarRocks 集群的平均导入速度。导入速度主要受限于集群的磁盘 I/O 及 BE 个数。

  Stream Load 还提供 `timeout` 参数来设置当前导入作业的超时时间。具体请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

## 使用说明

如果待导入数据文件中某行数据的某个字段缺失、并且 StarRocks 表中跟该字段对应的列定义为 `NOT NULL`，StarRocks 会在导入该行数据时自动往 StarRocks 表中对应的列补充 `NULL`。您也可以通过 [`ifnull()`](../sql-reference/sql-functions/condition-functions/ifnull.md) 函数指定要补充的默认值。

## 常见问题

请参见 [Stream Load 常见问题](../faq/loading/Stream_load_faq.md)。
