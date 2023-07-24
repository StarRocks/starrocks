# 导入和查询数据

本文介绍如何在 StarRocks 中导入和查询数据。

## 导入数据

为适配不同的数据导入需求，StarRocks 系统提供了五种不同的导入方式，以支持不同的数据源或者导入方式。

### Broker Load

Broker Load（参见 [从 HDFS 导入](../loading/hdfs_load.md)和[从云存储导入](../loading/cloud_storage_load.md)）是一种异步数据导入模式，通过 Broker 进程访问并读取外部数据源，然后采用 MySQL 协议向 StarRocks 创建导入作业。

Broker Load 模式适用于源数据在 Broker 进程可访问的存储系统（如 HDFS，S3）中的情景，可以支撑数据量达数百 GB 的导入作业。该导入方式支持的数据源有 Apache Hive™ 等。

### Spark Load

[Spark Load](/loading/SparkLoad.md) 是一种异步数据导入模式，通过外部的 Apache Spark™ 资源实现对导入数据的预处理，提高 StarRocks 大数据量的导入性能并且节省 StarRocks 集群的计算资源。

Spark Load 模式适用于初次向 StarRocks 迁移大数据量（TB 级别）的场景。该导入方式支持的数据源应位于 Apache Spark™ 可访问的存储系统（如 HDFS）中。

通过 Spark Load 可以基于 Apache Hive™ 表实现 [全局字典](/loading/SparkLoad.md) 的数据结构，对输入数据进行类型转换，保存原始值到编码值的映射，例如将字符串类型映射成整型。

### Stream Load

[Stream Load](/loading/StreamLoad.md) 是一种同步数据导入模式。用户通过 HTTP 协议发送请求将本地文件或数据流导入到 StarRocks 中，并等待系统返回导入的结果状态，从而判断导入是否成功。

Stream Load 模式适用于导入本地文件，或通过程序导入数据流中的数据。该导入方式支持的数据源有 Apache Flink®、CSV 文件等。

### Routine Load

[Routine Load](/loading/RoutineLoad.md)（例行导入）提供从指定数据源进行自动数据导入的功能。您可以通过 MySQL 协议提交例行导入作业，生成一个常驻线程，不间断地从数据源（如 Apache Kafka®）中读取数据并导入到 StarRocks 中。

### Insert Into

[Insert Into](/loading/InsertInto.md) 导入模式是一种同步数据导入模式，类似 MySQL 中的 Insert 语句，StarRocks 支持通过 `INSERT INTO tbl SELECT ...;` 的方式从 StarRocks 的表中读取数据并导入到另一张表。您也可以通过 `INSERT INTO tbl VALUES(...);` 插入单条数据。该导入方式支持的数据源有 DataX/DTS、Kettle/Informatic、以及 StarRocks 本身。

具体导入方式详情请参考 [数据导入](../loading/Loading_intro.md)。

### 通过 Stream Load 导入数据

以下示例以 Stream Load 导入方式为例，将文件中的数据导入到 [建表](/quick_start/Create_table.md) 章节中创建的 `detailDemo` 表中。

在本地创建数据文件 **detailDemo_data**，以逗号作为数据之间的分隔符，插入两条数据。具体内容如下：

```Plain Text
2022-03-13,1,1212,1231231231,123412341234,123452342342343324,hello,welcome,starrocks,2022-03-15 12:21:32,123.04,21.12345,123456.123456,true
2022-03-14,2,1212,1231231231,123412341234,123452342342343324,hello,welcome,starrocks,2022-03-15 12:21:32,123.04,21.12345,123456.123456,false
```

接着，以 "streamDemo" 为 Label，通过 curl 命令封装 HTTP 请求，将本地文件 **detailDemo_data** 导入 `detailDemo` 表。

```bash
curl --location-trusted -u <username>:<password> -T detailDemo_data -H "label: streamDemo" \
-H "column_separator:," \
http://127.0.0.1:8030/api/example_db/detailDemo/_stream_load
```

> **注意**
>
> - 如果账号没有设置密码，这里只需要传入 `<username>:`。
> - HTTP 地址中 IP 为 FE 节点 IP，端口为 **fe.conf** 中配置的 `http port`。

## 查询

StarRocks 兼容 MySQL 协议，其查询语句基本符合 SQL-92 标准。

### 简单查询

通过 MySQL 客户端登录 StarRocks，查询表中全部数据。

```sql
use example_db;
select * from detailDemo;
```

### 通过标准 SQL 查询

将查询结果以 `region_num` 字段降序排列。

```sql
select * from detailDemo order by region_num desc;
```

StarRocks 支持多种 select 用法，包括：[Join](/sql-reference/sql-statements/data-manipulation/SELECT.md#%E8%BF%9E%E6%8E%A5join)，[子查询](/sql-reference/sql-statements/data-manipulation/SELECT.md#子查询)，[With 子句](/sql-reference/sql-statements/data-manipulation/SELECT.md#with%E5%AD%90%E5%8F%A5) 等，详见 [查询章节](/sql-reference/sql-statements/data-manipulation/SELECT.md)。

## 扩展支持

StarRocks 拓展支持多种函数、视图、以及外部表。

### 函数

StarRocks 中支持多种函数，包括：[日期函数](/sql-reference/sql-functions/date-time-functions/convert_tz.md)，[地理位置函数](/sql-reference/sql-functions/spatial-functions/st_astext.md)，[字符串函数](/sql-reference/sql-functions/string-functions/append_trailing_char_if_absent.md)，[聚合函数](/sql-reference/sql-functions/aggregate-functions/approx_count_distinct.md)，[Bitmap 函数](/sql-reference/sql-functions/bitmap-functions/bitmap_and.md)，[数组函数](/sql-reference/sql-functions/array-functions/array_append.md)，[cast 函数](/sql-reference/sql-functions/cast.md)，[hash 函数](/sql-reference/sql-functions/hash-functions/murmur_hash3_32.md)，[加密函数](/sql-reference/sql-functions/crytographic-functions/md5.md)，[窗口函数](/sql-reference/sql-functions/Window_function.md) 等。

### 视图

StarRocks 支持创建 [逻辑视图](/sql-reference/sql-statements/data-definition/CREATE%20VIEW.md#description) 和 [物化视图](/using_starrocks/Materialized_view.md#物化视图)。具体使用方式详见对应章节。

### 外部表

StarRocks 支持多种外部表：[MySQL 外部表](/data_source/External_table.md#mysql-外部表)，[Elasticsearch 外部表](/data_source/External_table.md#elasticsearch-外部表)，[Apache Hive™ 外表](/data_source/External_table.md#hive-外表)，[StarRocks 外部表](/data_source/External_table.md#starrocks-外部表)，[Apache Iceberg 外表](/data_source/External_table.md#apache-iceberg-外表)，[Apache Hudi 外表](/data_source/External_table.md#apache-hudi-外表)。成功创建外部表后，可通过查询外部表的方式接入其他数据源。

## 慢查询分析

StarRocks 支持通过多种方式分析查询瓶颈以及优化查询效率。

### 通过调整并行度优化查询效率

我们推荐您通过设置 Pipeline 执行引擎变量。您也可以通过调整一个 [Fragment](/introduction/Features.md#mpp分布式执行框架) 实例的并行数量 `set  parallel_fragment_exec_instance_num = 8;` 来设置查询并行度，从而提高 CPU 资源利用率和查询效率。详细的参数介绍及设置，参考 [查询并行度相关参数](/administration/Query_management.md)。

### 查看 Profile 并分析查询瓶颈

- 查看查询计划。

  ```sql
  explain costs select * from detailDemo;
  ```

  > **注意**
  >
  > StarRocks 1.19 以前版本需使用 `explain sql` 查看查询计划。

- 开启 Profile 上报。

  ```sql
  set enable_profile = true;
  ```

  > **注意**
  >
  > 通过此方式设置 Profile 上报仅在当前 session 生效。

- 社区版用户可以通过 `http://FE_IP:FE_HTTP_PORT/query` 查看当前的查询和 Profile 信息。
- 企业版用户可以在 StarRocks Manager 的查询页面查看图形化 Profile 展示，点击查询链接可以在 **执行时间** 页面看到树状展示，并可以在 **执行详情** 页面看到完整的 Profile 详细信息。如果以上方法仍达不到预期，您可以发送执行详情页面的文本到社区或者技术支持群以寻求帮助。

有关 Plan 和 Profile 的详细介绍，参考 [查询分析](../administration/Query_planning.md) 和 [性能优化](../administration/Profiling.md) 章节。
