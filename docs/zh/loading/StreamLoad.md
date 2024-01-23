---
displayed_sidebar: "Chinese"
---

# 从本地文件系统导入

import InsertPrivNote from '../assets/commonMarkdown/insertPrivNote.md'

StarRocks 提供两种导入方式帮助您从本地文件系统导入数据：

- 使用 [Stream Load](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md) 进行同步导入。
- 使用 [Broker Load](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 进行异步导入。

两种导入方式各有优势：

- Stream Load 支持 CSV 和 JSON 两种数据文件格式，适用于数据文件数量较少且单个文件的大小不超过 10 GB 的场景。
- Broker Load 支持 Parquet、ORC、及 CSV 三种文件格式，适用于数据文件数量较多且单个文件的大小超过 10 GB 的场景、以及文件存储在 NAS 的场景。**但是该功能自 v2.5 起支持，而且这种导入方式需要您在数据所在的机器上[部署 Broker](../deployment/deploy_broker.md)。**

对于 CSV 格式的数据，需要注意以下两点：

- StarRocks 支持设置长度最大不超过 50 个字节的 UTF-8 编码字符串作为列分隔符，包括常见的逗号 (,)、Tab 和 Pipe (|)。
- 空值 (null) 用 `\N` 表示。比如，数据文件一共有三列，其中某行数据的第一列、第三列数据分别为 `a` 和 `b`，第二列没有数据，则第二列需要用 `\N` 来表示空值，写作 `a,\N,b`，而不是 `a,,b`。`a,,b` 表示第二列是一个空字符串。

Stream Load 和 Broker Load 均支持在导入过程中做数据转换、以及通过 UPSERT 和 DELETE 操作实现数据变更。请参见[导入过程中实现数据转换](../loading/Etl_in_loading.md)和[通过导入实现数据变更](../loading/Load_to_Primary_Key_tables.md)。

## 准备工作

### 查看权限

<InsertPrivNote />

## 使用 Stream Load 从本地导入

Stream Load 是一种基于 HTTP PUT 的同步导入方式。提交导入作业以后，StarRocks 会同步地执行导入作业，并返回导入作业的结果信息。您可以通过返回的结果信息来判断导入作业是否成功。

> **NOTICE**
>
> Stream Load 操作会同时更新和 StarRocks 原始表相关的物化视图的数据。

### 基本原理

您需要在客户端上通过 HTTP 发送导入作业请求给 FE，FE 会通过 HTTP 重定向 (Redirect) 指令将请求转发给某一个 BE。或者，您也可以直接发送导入作业请求给某一个 BE。

:::note

如果把导入作业请求发送给 FE，FE 会通过轮询机制选定由哪一个 BE 来接收请求，从而实现 StarRocks 集群内的负载均衡。因此，推荐您把导入作业请求发送给 FE。

:::

接收导入作业请求的 BE 作为 Coordinator BE，将数据按表结构划分、并分发数据到其他各相关的 BE。导入作业的结果信息由 Coordinator BE 返回给客户端。需要注意的是，如果您在导入过程中停止 Coordinator BE，会导致导入作业失败。

下图展示了 Stream Load 的主要流程：

![Stream Load 原理图](../assets/4.2-1-zh.png)

### 使用限制

Stream Load 当前不支持导入某一列为 JSON 的 CSV 文件的数据。

### 操作示例

本文以 curl 工具为例，介绍如何使用 Stream Load 从本地文件系统导入 CSV 或 JSON 格式的数据。有关创建导入作业的详细语法和参数说明，请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

注意在 StarRocks 中，部分文字是 SQL 语言的保留关键字，不能直接用于 SQL 语句。如果想在 SQL 语句中使用这些保留关键字，必须用反引号 (`) 包裹起来。参见[关键字](../sql-reference/sql-statements/keywords.md)。

#### 导入 CSV 格式的数据

##### 数据样例

在本地文件系统中创建一个 CSV 格式的数据文件 `example1.csv`。文件一共包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

```Plain_Text
1,Lily,23
2,Rose,23
3,Alice,24
4,Julia,25
```

##### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建主键模型表 `table1`，包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户姓名和用户得分，主键为 `id` 列，如下所示：

```SQL
CREATE TABLE `table1`
(
    `id` int(11) NOT NULL COMMENT "用户 ID",
    `name` varchar(65533) NULL COMMENT "用户姓名",
    `score` int(11) NOT NULL COMMENT "用户得分"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`);
```

:::note

自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

:::

##### 提交导入作业

通过如下命令，把 `example1.csv` 文件中的数据导入到 `table1` 表中：

```Bash
curl --location-trusted -u <username>:<password> -H "label:123" \
    -H "Expect:100-continue" \
    -H "column_separator:," \
    -H "columns: id, name, score" \
    -T example1.csv -XPUT \
    http://<fe_host>:<fe_http_port>/api/mydatabase/table1/_stream_load
```

:::note

- 如果账号没有设置密码，这里只需要传入 `<username>:`。
- 您可以通过 [SHOW FRONTENDS](../sql-reference/sql-statements/Administration/SHOW_FRONTENDS.md) 命令查看 FE 节点的 IP 地址和 HTTP 端口号。

:::

`example1.csv` 文件中包含三列，跟 `table1` 表的 `id`、`name`、`score` 三列一一对应，并用逗号 (,) 作为列分隔符。因此，需要通过 `column_separator` 参数指定列分隔符为逗号 (,)，并且在 `columns` 参数中按顺序把 `example1.csv` 文件中的三列临时命名为 `id`、`name`、`score`。`columns` 参数中声明的三列，按名称对应 `table1` 表中的三列。

导入完成后，您可以查询 `table1` 表，验证数据导入是否成功，如下所示：

```SQL
SELECT * FROM table1;

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

在本地文件系统中创建一个 JSON 格式的数据文件 `example2.json`。文件一共包含两个字段，分别代表城市名称和城市 ID，如下所示：

```JSON
{"name": "北京", "code": 2}
```

##### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建主键模型表 `table2`，包含 `id` 和 `city` 两列，分别代表城市 ID 和城市名称，主键为 `id` 列，如下所示：

```SQL
CREATE TABLE `table2`
(
    `id` int(11) NOT NULL COMMENT "城市 ID",
    `city` varchar(65533) NULL COMMENT "城市名称"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`);
```

:::note

自 2.5.7 版本起，StarRocks 支持在建表和新增分区时自动设置分桶数量 (BUCKETS)，您无需手动设置分桶数量。更多信息，请参见 [确定分桶数量](../table_design/Data_distribution.md#确定分桶数量)。

:::

##### 提交导入作业

通过如下语句把 `example2.json` 文件中的数据导入到 `table2` 表中：

```Bash
curl -v --location-trusted -u <username>:<password> -H "strict_mode: true" \
    -H "Expect:100-continue" \
    -H "format: json" -H "jsonpaths: [\"$.name\", \"$.code\"]" \
    -H "columns: city,tmp_id, id = tmp_id * 100" \
    -T example2.json -XPUT \
    http://<fe_host>:<fe_http_port>/api/mydatabase/table2/_stream_load
```

:::note

- 如果账号没有设置密码，这里只需要传入 `<username>:`。
- 您可以通过 [SHOW FRONTENDS](../sql-reference/sql-statements/Administration/SHOW_FRONTENDS.md) 命令查看 FE 节点的 IP 地址和 HTTP 端口号。

:::

`example2.json` 文件中包含 `name` 和 `code` 两个键，跟 `table2` 表中的列之间的对应关系如下图所示。

![JSON 映射图](../assets/4.2-2.png)

上图所示的对应关系描述如下：

- 提取 `example2.json` 文件中包含的 `name` 和 `code` 两个字段，按顺序依次映射到 `jsonpaths` 参数中声明的 `name` 和 `code` 两个字段。
- 提取 `jsonpaths` 参数中声明的 `name` 和 `code` 两个字段，**按顺序映射**到 `columns` 参数中声明的 `city` 和 `tmp_id` 两列。
- 提取 `columns` 参数声明中的 `city` 和 `id` 两列，**按名称映射**到 `table2` 表中的 `city` 和 `id` 两列。

:::note

上述示例中，在导入过程中先将 `example2.json` 文件中 `code` 字段对应的值乘以 100，然后再落入到 `table2` 表的 `id` 中。

:::

有关导入 JSON 数据时 `jsonpaths`、`columns` 和 StarRocks 表中的字段之间的对应关系，请参见 STREAM LOAD 文档中“[列映射](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#列映射)”章节。

导入完成后，您可以查询 `table2` 表，验证数据导入是否成功，如下所示：

```SQL
SELECT * FROM table2;
+------+--------+
| id   | city   |
+------+--------+
| 200  | 北京    |
+------+--------+
4 rows in set (0.01 sec)
```

#### 查看 Stream Load 导入进度

导入作业结束后，StarRocks 会以 JSON 格式返回本次导入作业的结果信息，具体请参见 STREAM LOAD 文档中“[返回值](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md#返回值)”章节。

Stream Load 不支持通过 SHOW LOAD 语句查看导入作业执行情况。

#### 取消 Stream Load 作业

Stream Load 不支持手动取消导入作业。如果导入作业发生超时或者导入错误，StarRocks 会自动取消该作业。

### 参数配置

这里介绍使用 Stream Load 导入方式需要注意的一些系统参数配置。这些参数作用于所有 Stream Load 导入作业。

- `streaming_load_max_mb`：单个源数据文件的大小上限。默认文件大小上限为 10 GB。具体请参见[配置 BE 动态参数](../administration/Configuration.md#配置-be-动态参数)。

  建议一次导入的数据量不要超过 10 GB。如果数据文件的大小超过 10 GB，建议您拆分成若干小于 10 GB 的文件分次导入。如果由于业务场景需要，无法拆分数据文件，可以适当调大该参数的取值，从而提高数据文件的大小上限。

  需要注意的是，如果您调大该参数的取值，需要重启 BE 才能生效，并且系统性能有可能会受影响，并且也会增加失败重试时的代价。

  :::note

  导入 JSON 格式的数据时，需要注意以下两点：

  - 单个 JSON 对象的大小不能超过 4 GB。如果 JSON 文件中单个 JSON 对象的大小超过 4 GB，会提示 "This parser can't support a document that big." 错误。
  - HTTP 请求中 JSON Body 的大小默认不能超过 100 MB。如果 JSON Body 的大小超过 100 MB，会提示 "The size of this batch exceed the max size [104857600] of json type data data [8617627793]. Set ignore_json_size to skip check, although it may lead huge memory consuming." 错误。为避免该报错，可以在 HTTP 请求头中添加 `"ignore_json_size:true"` 设置，忽略对 JSON Body 大小的检查。

  :::

- `stream_load_default_timeout_second`：导入作业的超时时间。默认超时时间为 600 秒。具体请参见[配置 FE 动态参数](../administration/Configuration.md#配置-fe-动态参数)。

  如果您创建的导入作业经常发生超时，可以通过该参数适当地调大超时时间。您可以通过如下公式计算导入作业的超时时间：

  **导入作业的超时时间 > 待导入数据量/平均导入速度**

  例如，如果源数据文件的大小为 10 GB，并且当前 StarRocks 集群的平均导入速度为 100 MB/s，则超时时间应该设置为大于 100 秒。

  :::note
  
  “平均导入速度”是指目前 StarRocks 集群的平均导入速度。导入速度主要受限于集群的磁盘 I/O 及 BE 个数。

  :::

  Stream Load 还提供 `timeout` 参数来设置当前导入作业的超时时间。具体请参见 [STREAM LOAD](../sql-reference/sql-statements/data-manipulation/STREAM_LOAD.md)。

### 使用说明

如果数据文件中某条数据记录的某个字段缺失、并且该字段对应的目标表中的列定义为 `NOT NULL`，则 StarRocks 在导入该字段时自动往目标表中对应的列填充 `NULL` 值。您也可以在导入命令中通过 `ifnull()` 函数指定您想要填充的默认值。

例如，如果上面示例里数据文件 `example2.json` 中代表城市 ID 的字段缺失、并且您想在该字段对应的目标表中的列中填充 `x`，您可以在导入命令中指定 `"columns: city, tmp_id, id = ifnull(tmp_id, 'x')"`。

## 使用 Broker Load 从本地导入

除 Stream Load 以外，您还可以通过 Broker Load 从本地导入数据。该功能自 v2.5 起支持。

Broker Load 是一种异步导入方式。提交导入作业以后，StarRocks 会异步地执行导入作业，不会直接返回作业结果，您需要手动查询作业结果。参见[查看 Broker Load 导入进度](#查看-broker-load-导入进度)。

### 使用限制

- 目前只能从单个 Broker 中导入数据，并且 Broker 版本必须为 2.5 及以后。
- 并发访问单点的 Broker 容易成为瓶颈，并发越高反而越容易造成超时、OOM 等问题。您可以通过设置 `pipeline_dop`（参见[调整查询并发度](../administration/Query_management.md#调整查询并发度)）来限制 Broker Load 的并行度，对于单个 Broker 的访问并行度建议设置小于 `16`。

### 准备工作

在使用 Broker Load 从本地文件系统导入数据前，需要完成如下准备工作：

1. 按照“[部署前提条件](../deployment/deployment_prerequisites.md)”、“[检查环境配置](../deployment/environment_configurations.md)”和“[准备部署文件](../deployment/prepare_deployment_files.md)”中的介绍，在本地文件所在机器上完成必要的环境配置。然后，在该机器上部署一个 Broker。具体操作跟在 BE 节点上部署一样，参见[部署 Broker 节点](../deployment/deploy_broker.md)。

   > **NOTICE**
   >
   > 只能从单个 Broker 中导入数据，并且 Broker 版本必须为 2.5 及以后。

2. 通过 [ALTER SYSTEM](../sql-reference/sql-statements/Administration/ALTER_SYSTEM.md#broker) 语句在 StarRocks 中添加上一步骤中部署好的 Broker（如 `172.26.199.40:8000`），并给 Broker 指定新名称（如 `sole_broker`）：

   ```SQL
   ALTER SYSTEM ADD BROKER sole_broker "172.26.199.40:8000";
   ```

### 操作示例

Broker Load 支持导入单个数据文件到单张表、导入多个数据文件到单张表、以及导入多个数据文件分别到多张表。这里以导入多个数据文件到单张表为例。

注意在 StarRocks 中，部分文字是 SQL 语言的保留关键字，不能直接用于 SQL 语句。如果想在 SQL 语句中使用这些保留关键字，必须用反引号 (`) 包裹起来。参见[关键字](../sql-reference/sql-statements/keywords.md)。

#### 数据样例

以 CSV 格式的数据为例，登录本地文件系统，在指定路径（假设为 `/user/starrocks/`）下创建两个 CSV 格式的数据文件，`file1.csv` 和 `file2.csv`。两个数据文件都包含三列，分别代表用户 ID、用户姓名和用户得分，如下所示：

- `file1.csv`

  ```Plain
  1,Lily,21
  2,Rose,22
  3,Alice,23
  4,Julia,24
  ```

- `file2.csv`

  ```Plain
  5,Tony,25
  6,Adam,26
  7,Allen,27
  8,Jacky,28
  ```

#### 建库建表

通过如下语句创建数据库、并切换至该数据库：

```SQL
CREATE DATABASE IF NOT EXISTS mydatabase;
USE mydatabase;
```

通过如下语句手动创建主键模型表 `mytable`，包含 `id`、`name` 和 `score` 三列，分别代表用户 ID、用户姓名和用户得分，主键为 `id` 列，如下所示：

```SQL
DROP TABLE IF EXISTS `mytable`

CREATE TABLE `mytable`
(
    `id` int(11) NOT NULL COMMENT "用户 ID",
    `name` varchar(65533) NULL DEFAULT "" COMMENT "用户姓名",
    `score` int(11) NOT NULL DEFAULT "0" COMMENT "用户得分"
)
ENGINE=OLAP
PRIMARY KEY(`id`)
DISTRIBUTED BY HASH(`id`)
PROPERTIES("replication_num"="1");
```

#### 提交导入作业

通过如下语句，把本地文件系统的 `/user/starrocks/` 路径下所有数据文件（`file1.csv` 和 `file2.csv`）的数据导入到目标表 `mytable`：

```SQL
LOAD LABEL mydatabase.label_local
(
    DATA INFILE("file:///home/disk1/business/csv/*")
    INTO TABLE mytable
    COLUMNS TERMINATED BY ","
    (id, name, score)
)
WITH BROKER "sole_broker"
PROPERTIES
(
    "timeout" = "3600"
);
```

导入语句包含四个部分：

- `LABEL`：导入作业的标签，字符串类型，可用于查询导入作业的状态。
- `LOAD` 声明：包括源数据文件所在的 URI、源数据文件的格式、以及目标表的名称等作业描述信息。
- `BROKER`：Broker 的名称。
- `PROPERTIES`：用于指定超时时间等可选的作业属性。

有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

#### 查看 Broker Load 导入进度

在 v3.0 及以前版本，您需要通过 [SHOW LOAD](../sql-reference/sql-statements/data-manipulation/SHOW_LOAD.md) 语句或者 curl 命令来查看导入作业的进度。

在 v3.1 及以后版本，您可以通过 [`information_schema.loads`](../administration/information_schema.md#loads) 视图来查看 Broker Load 作业的进度：

```SQL
SELECT * FROM information_schema.loads;
```

如果您提交了多个导入作业，您可以通过 `LABEL` 过滤出想要查看的作业。例如：

```SQL
SELECT * FROM information_schema.loads WHERE LABEL = 'label_local';
```

确认导入作业完成后，您可以从表内查询数据，验证数据导入是否成功。例如：

```SQL
SELECT * FROM mytable;
+------+-------+-------+
| id   | name  | score |
+------+-------+-------+
|    3 | Alice |    23 |
|    5 | Tony  |    25 |
|    6 | Adam  |    26 |
|    1 | Lily  |    21 |
|    2 | Rose  |    22 |
|    4 | Julia |    24 |
|    7 | Allen |    27 |
|    8 | Jacky |    28 |
+------+-------+-------+
8 rows in set (0.07 sec)
```

#### 取消 Broker Load 作业

当导入作业状态不为 **CANCELLED** 或 **FINISHED** 时，可以通过 [CANCEL LOAD](../sql-reference/sql-statements/data-manipulation/CANCEL_LOAD.md) 语句来取消该导入作业。

例如，可以通过以下语句，撤销 `mydatabase` 数据库中标签为 `label_local` 的导入作业：

```SQL
CANCEL LOAD
FROM mydatabase
WHERE LABEL = "label_local";
```

## 使用 Broker Load 从 NAS 导入

从 NAS 导入数据时，有两种方法：

- 把 NAS 当做本地文件系统，然后按照前面“[使用 Broker Load 从本地导入](#使用-broker-load-从本地导入)”里介绍的方法来实现导入。
- 【推荐】把 NAS 作为云存储设备，这样无需部署 Broker 即可通过 Broker Load 实现导入。

本小节主要介绍这种方法。具体操作步骤如下：

1. 把 NAS 挂载到所有的 BE、FE 节点，同时保证所有节点的挂载路径完全一致。这样，所有 BE 可以像访问 BE 自己的本地文件一样访问 NAS。

2. 使用 Broker Load 导入数据。

   ```SQL
   LOAD LABEL test_db.label_nas
   (
       DATA INFILE("file:///home/disk1/sr/*")
       INTO TABLE mytable
       COLUMNS TERMINATED BY ","
   )
   WITH BROKER
   PROPERTIES
   (
       "timeout" = "3600"
   );
   ```

   导入语句包含四个部分：

   - `LABEL`：导入作业的标签，字符串类型，可用于查询导入作业的状态。
   - `LOAD` 声明：包括源数据文件所在的 URI、源数据文件的格式、以及目标表的名称等作业描述信息。注意，`DATA INFILE` 这里用于指定挂载路径，如上面示例所示，`file:///` 为前缀，`/home/disk1/sr` 为挂载路径。
   - `BROKER`：无需指定。
   - `PROPERTIES`：用于指定超时时间等可选的作业属性。

   有关详细的语法和参数说明，参见 [BROKER LOAD](../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)。

提交导入作业后，您可以查看导入进度、或者取消导入作业。具体操作参见本文“[查看 Broker Load 导入进度](#查看-broker-load-导入进度)”和“[取消 Broker Load 作业](#取消-broker-load-作业)中的介绍。
