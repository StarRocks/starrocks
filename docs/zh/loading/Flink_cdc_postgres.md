---
sidebar_position: 101
displayed_sidebar: docs
keywords:
  - PostgreSQL
  - postgres
  - sync
  - Flink CDC
description: "使用 Flink CDC pipeline 捕获 PostgreSQL 变更数据，并实时同步至 StarRocks 主键表。"
---

# 从 PostgreSQL 实时同步

import InsertPrivNote from '../_assets/commonMarkdown/insertPrivNote.mdx'
import FlinkStarRocksConnection from '../_assets/commonMarkdown/Edition_Specific_Flink_StarRocks_Connection.mdx'

本文介绍如何通过 [Flink CDC](https://nightlies.apache.org/flink/flink-cdc-docs-stable/) pipeline 将 PostgreSQL 的数据实时同步至 StarRocks。Pipeline 先复制每张表中已有的数据，然后在秒级内持续应用 PostgreSQL 中的每一条 INSERT、UPDATE 和 DELETE。Pipeline 会自动创建 StarRocks 表，因此无需单独同步库表结构。

<InsertPrivNote />

## 基本原理

Flink CDC pipeline 是一个 YAML 文件，包含 **source**（PostgreSQL）、**sink**（StarRocks），以及可选的 **route** 和 **transform** 规则。将该文件提交到 Flink 集群后，它会作为一个 Flink 作业运行：

1. PostgreSQL source 为每张匹配的表创建快照，然后通过逻辑复制槽（replication slot）从 PostgreSQL 预写日志（WAL）中读取变更。
2. StarRocks sink 为 StarRocks 中尚不存在的每张源表创建一张[主键表](../table_design/table_types/primary_key_table.md)，然后通过 [Stream Load](./StreamLoad.md) 导入数据。由于每张表都有主键，更新会覆盖已有的行，删除会移除对应的行。

数据投递语义为 at-least-once：发生故障后，部分变更可能会被再次写入，而主键表保证重复写入不会产生影响。

:::note

Pipeline 不会同步 PostgreSQL 的表结构变更。参见[变更表结构](#变更表结构)。

:::

## 准备工作

您需要：

- PostgreSQL 10 或更高版本。本文使用 PostgreSQL 内置的 `pgoutput` 逻辑解码插件，无需安装服务端扩展。
- 一个 StarRocks 集群。
- 一个 Flink 1.20 集群，以及运行它所需的 Java 11 或 17。
- 每个 Flink TaskManager 都能通过网络访问 PostgreSQL 的端口（默认 `5432`），并按照[连接 StarRocks](#连接-starrocks) 中的说明访问 StarRocks。
- 要同步的每张 PostgreSQL 表都有主键。StarRocks 会使用相同的主键创建目标表。

## 第一步：准备 PostgreSQL

请以 PostgreSQL 超级用户或表的所有者身份执行以下步骤。示例同步的是 `shop` 数据库中 `public` schema 下的表。

1. 开启逻辑复制。在 `postgresql.conf` 中将 `wal_level` 设置为 `logical`：

   ```properties
   wal_level = logical
   # 每个运行中的 pipeline 使用一个复制槽和一个 WAL sender。
   max_replication_slots = 4
   max_wal_senders = 4
   ```

   修改 `wal_level` 后需要重启 PostgreSQL。对于 Amazon RDS、Cloud SQL 等托管服务，请在实例的参数组中设置对应的参数（例如 `rds.logical_replication = 1`）。

   重启后检查该设置：

   ```sql
   SHOW wal_level;
   ```

   ```plaintext
    wal_level
   -----------
    logical
   ```

2. 为 Flink CDC 创建用户。该用户需要 `REPLICATION` 属性以及表的读取权限：

   ```sql
   CREATE ROLE flink_cdc WITH LOGIN REPLICATION PASSWORD '<password>';
   GRANT CONNECT ON DATABASE shop TO flink_cdc;
   GRANT USAGE ON SCHEMA public TO flink_cdc;
   GRANT SELECT ON ALL TABLES IN SCHEMA public TO flink_cdc;
   ```

   如果 `pg_hba.conf` 限制了复制连接，请允许该用户从 Flink TaskManager 所在主机连接。例如：

   ```plaintext
   # TYPE  DATABASE     USER       ADDRESS          METHOD
   host    shop         flink_cdc  10.0.0.0/24      scram-sha-256
   host    replication  flink_cdc  10.0.0.0/24      scram-sha-256
   ```

3. 为要同步的表创建 publication。Publication 必须包含 pipeline 读取的所有表（即[第四步](#第四步定义-pipeline)中的 `tables` 配置项）：

   ```sql
   CREATE PUBLICATION flink_pub FOR TABLE public.orders, public.customers;
   ```

   :::tip

   请自行创建 publication。如果 publication 不存在，connector 会尝试执行 `CREATE PUBLICATION ... FOR ALL TABLES`，而只有超级用户才能执行该语句。对于其他用户，该语句会失败，Flink 会不断重试，直到报告的错误变成 `max_wal_senders`，而不是缺少 publication。

   :::

4. 将每张表的 replica identity 设置为 `FULL`，使 WAL 为每条 UPDATE 和 DELETE 记录完整的旧行：

   ```sql
   ALTER TABLE public.orders REPLICA IDENTITY FULL;
   ALTER TABLE public.customers REPLICA IDENTITY FULL;
   ```

   请在启动 pipeline 之前完成此设置。使用默认的 replica identity 时，第一条 UPDATE 或 DELETE 会导致 Flink 作业因 `NullPointerException` 失败，并且作业每次重启后都会在这条变更上再次失败。

## 第二步：准备 StarRocks

创建目标数据库以及 pipeline 使用的用户：

```sql
CREATE DATABASE shop;

CREATE USER flink_sink IDENTIFIED BY '<password>';
GRANT CREATE TABLE ON DATABASE shop TO USER flink_sink;
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON ALL TABLES IN DATABASE shop TO USER flink_sink;
```

## 第三步：安装 Flink CDC

1. 下载并启动 Flink 1.20。参见 Flink 文档中的 [First steps](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/try-flink/local_installation/)。

2. 在 Flink 配置文件 `conf/config.yaml` 中添加以下配置以开启 Checkpoint，然后重启集群：

   ```yaml
   execution.checkpointing.interval: 10s
   ```

   PostgreSQL source 会在 Checkpoint 完成时向 PostgreSQL 报告其在 WAL 中的位置。如果不开启 Checkpoint，复制槽会保留 pipeline 启动以来的所有 WAL 段，最终写满 PostgreSQL 的磁盘。

3. 从 [Flink CDC 下载页面](https://flink.apache.org/downloads/#apache-flink-cdc)下载与您的 Flink 版本匹配的 Flink CDC 版本，并解压。从 Flink CDC 3.6 开始，每个发布包只针对一个 Flink 版本构建，因此对于 Flink 1.20，请下载 `flink-cdc-3.6.0-1.20-bin.tar.gz`，而不是 `-2.2` 的包。

   ```bash
   tar -xzf flink-cdc-3.6.0-1.20-bin.tar.gz
   cd flink-cdc-3.6.0-1.20
   ```

4. 从 Maven Central 下载 PostgreSQL 和 StarRocks pipeline connector 到 Flink CDC 的 `lib` 目录。请使用与 Flink CDC 发布包相同的版本：

   ```bash
   cd lib
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-postgres/3.6.0-1.20/flink-cdc-pipeline-connector-postgres-3.6.0-1.20.jar
   wget https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-starrocks/3.6.0-1.20/flink-cdc-pipeline-connector-starrocks-3.6.0-1.20.jar
   cd ..
   ```

## 第四步：定义 pipeline

创建名为 `postgres-to-starrocks.yaml` 的文件：

```yaml
source:
  type: postgres
  name: PostgreSQL source
  hostname: <postgres_host>
  port: 5432
  username: flink_cdc
  password: <password>
  # 以逗号分隔的 <database>.<schema>.<table>，表名部分可以是正则表达式。
  # 请列出与 publication 相同的表。
  tables: shop.public.orders, shop.public.customers
  # 要创建并使用的复制槽。只能包含小写字母、数字和下划线。
  slot.name: flink_starrocks
  decoding.plugin.name: pgoutput
  debezium.publication.name: flink_pub

sink:
  type: starrocks
  name: StarRocks sink
  # StarRocks 地址的设置方法，参见下文“连接 StarRocks”。
  jdbc-url: jdbc:mysql://<fe_host>:9030
  load-url: <fe_host>:8030
  username: flink_sink
  password: <password>
  # 默认值为 300000（5 分钟）。较小的值可以让变更更快可见。
  sink.buffer-flush.interval-ms: 5000

route:
  # 将 public schema 下的每张表写入 StarRocks 数据库 shop。
  - source-table: public.\.*
    sink-table: shop.<>
    replace-symbol: <>

pipeline:
  name: PostgreSQL to StarRocks
  parallelism: 1
```

请注意以下配置：

- `tables` 必须与 publication 一致。快照会复制 `tables` 选中的所有表，但 PostgreSQL 只会为 publication 中的表推送变更。不在 publication 中的表只会被复制一次，之后不再更新，作业也不会报告任何错误。
- `tables` 使用 `<database>.<schema>.<table>` 的格式指定表。但在 pipeline 内部，PostgreSQL 表只用 `<schema>.<table>` 标识，因此 `route` 和 `transform` 规则应匹配 `public.orders`，而不是 `shop.public.orders`。
- 如果没有 `route` 配置，sink 会将每张表写入以 PostgreSQL **schema** 命名的 StarRocks 数据库，因此 `public` 下的表会写入名为 `public` 的数据库。
- `sink.buffer-flush.interval-ms` 的默认值为 5 分钟。使用默认值时，新启动的 pipeline 在几分钟内看起来没有任何动作。
- 如需为 sink 创建的表设置属性，请在 `sink` 部分添加 `table.create.properties.<property>`，例如 `table.create.properties.replication_num: 1`。

有关 source 和 sink 的所有配置项，请参见 Flink CDC 的 [Postgres connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/postgres/) 和 [StarRocks connector](https://nightlies.apache.org/flink/flink-cdc-docs-stable/docs/connectors/pipeline-connectors/starrocks/) 参考文档。

### 连接 StarRocks

<FlinkStarRocksConnection />

### 默认值为函数的列

Sink 会将 PostgreSQL 中每一列的默认值复制到 StarRocks 的 CREATE TABLE 语句中。函数调用形式的默认值（例如 `DEFAULT now()`）在 StarRocks 中无效，作业会失败并报类似以下的错误：

```plaintext
Invalid default value for 'created_at': date literal [now()] is invalid.
```

事先手动创建 StarRocks 表并不能避免该错误，因为 sink 仍然会发送 CREATE TABLE 语句。请改为添加一条 `transform` 规则重新计算该列。计算列没有默认值：

```yaml
transform:
  - source-table: public.orders
    projection: order_id, customer, amount, status, CAST(created_at AS TIMESTAMP(6)) AS created_at
```

请在 `projection` 中列出表的所有列。未列出的列不会被同步。

## 第五步：启动 pipeline

将 `FLINK_HOME` 设置为 Flink 的安装目录，然后在 Flink CDC 目录下提交 pipeline：

```bash
export FLINK_HOME=/path/to/flink-1.20
bin/flink-cdc.sh postgres-to-starrocks.yaml
```

```plaintext
Pipeline has been submitted to cluster.
Job ID: cda97bac3b504442d8106a2d70c6074c
Job Description: PostgreSQL to StarRocks
```

该作业会显示在 Flink Web UI 中（默认地址为 `http://<jobmanager_host>:8081`），其状态应保持为 `RUNNING`。如果状态变为 `RESTARTING`，请打开作业的 **Exceptions** 页签，并参见[常见问题排查](#常见问题排查)。

## 第六步：验证同步结果

1. 快照完成后，已有的数据会出现在 StarRocks 中：

   ```sql
   SELECT * FROM shop.orders ORDER BY order_id;
   ```

2. 在 PostgreSQL 中修改一些数据：

   ```sql
   INSERT INTO public.orders (order_id, customer, amount, status) VALUES (5, 'erin', 42.00, 'new');
   UPDATE public.orders SET status = 'returned' WHERE order_id = 3;
   DELETE FROM public.orders WHERE order_id = 4;
   ```

3. 几秒钟后（刷新间隔加上导入耗时），在 StarRocks 中再次执行查询。结果与在 PostgreSQL 中执行相同查询的结果一致。

## 管理 pipeline

### 监控复制槽

在 pipeline 确认已读取 WAL 之前，复制槽会让 PostgreSQL 服务器一直保留这些 WAL。当 pipeline 停止或落后时，WAL 会不断累积。查看复制槽保留的 WAL 大小：

```sql
SELECT slot_name,
       active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained_wal
FROM pg_replication_slots;
```

如果永久停止某个 pipeline，请删除其复制槽，使 PostgreSQL 可以释放 WAL：

```sql
SELECT pg_drop_replication_slot('flink_starrocks');
```

### 变更表结构

Pipeline 不会获取 PostgreSQL 的表结构变更。如果在 PostgreSQL 中新增一列，作业会继续运行，但不会写入新列，即使您在 StarRocks 表中也添加了该列。

表结构变更后，按以下步骤同步该表：

1. 对 StarRocks 表执行相同的变更，例如使用 [ALTER TABLE](../sql-reference/sql-statements/table_bucket_part_index/ALTER_TABLE.md)。
2. [重新创建快照](#重新创建快照)。

### 重新创建快照

按以下步骤根据 PostgreSQL 的当前数据重新复制各表，然后恢复同步：

1. 取消 Flink 作业。
2. 删除复制槽，参见[监控复制槽](#监控复制槽)。
3. 清空 pipeline 写入的每一张 StarRocks 表：

   ```sql
   TRUNCATE TABLE shop.orders;
   TRUNCATE TABLE shop.customers;
   ```

4. 重新提交 pipeline。Pipeline 会为每张表重新创建快照，然后继续同步变更。

请不要跳过清空表的步骤。从取消作业到新快照开始之间在 PostgreSQL 中发生的变更不会被重放。快照会覆盖仍然存在的行，但在这段时间内于 PostgreSQL 中删除的行会一直保留在 StarRocks 中。在快照完成之前，被清空的表为空或只有部分数据。

## 常见问题排查

当作业状态为 `RESTARTING` 时，Flink Web UI 的 **Exceptions** 页签会显示原因。请查看最内层的 `Caused by` 行。

| 错误 | 原因及解决方法 |
| --- | --- |
| `number of requested standby connections exceeds max_wal_senders` | 通常是之前某个失败的表现，因为每次重试都会打开一个复制连接。请在 PostgreSQL 服务器日志中查找最早的错误。常见的是 `CREATE PUBLICATION` 报 `permission denied for database`，这说明 publication 不存在。参见[第一步](#第一步准备-postgresql)。 |
| `DebeziumSchemaDataTypeInference` 或 `extractBeforeDataRecord` 中出现 `NullPointerException` | 某张表未设置 `REPLICA IDENTITY FULL`。设置后，[重新创建快照](#重新创建快照)。 |
| `Invalid default value for '<column>'` | 某列在 PostgreSQL 中的默认值是函数。参见[默认值为函数的列](#默认值为函数的列)。 |
| `Connect to <host>:8040 ... Connection refused` | TaskManager 无法访问 FE 重定向导入请求的目标 BE 或 CN。请确保 TaskManager 能够通过 BE 和 CN 注册时使用的地址访问它们的 HTTP 端口。 |
| 作业状态为 `RUNNING`，但 StarRocks 中没有数据 | 请等待一个刷新间隔。`sink.buffer-flush.interval-ms` 的默认值为 5 分钟。 |
| 数据写入了名为 `public` 的数据库 | Pipeline 中没有 `route` 规则。参见[第四步](#第四步定义-pipeline)。 |

## 相关文档

- [从 MySQL 实时同步](./Flink_cdc_load.md)
- [从 Apache Flink® 持续导入](./Flink-connector-starrocks.md)
- [通过导入实现数据变更](./Load_to_Primary_Key_tables.md)
