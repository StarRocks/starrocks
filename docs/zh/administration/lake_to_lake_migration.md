---
displayed_sidebar: docs
---

# 存算分离集群跨集群数据迁移工具

StarRocks 跨集群数据迁移工具支持在两个存算分离（云原生）StarRocks 集群之间迁移数据。迁移过程中，目标集群的计算节点（CN）直接从源集群的对象存储将数据文件拷贝到目标集群的对象存储，无需 BE 间的网络传输。

:::note

- 本文仅适用于从**存算分离**源集群迁移到**存算分离**目标集群的场景。如需从存算一体集群迁移，请参考[跨集群数据迁移工具](./data_migration_tool.md)。
- 目标集群版本必须为 v4.1 及以上。
- 目标集群不能是存算一体集群。

:::

## 准备工作

### 源集群配置

迁移过程中，源集群的自动 Vacuum 机制可能会删除目标 CN 仍需读取的历史数据版本。在开始迁移前，您需要延长 Vacuum 宽限期以防止此问题。

1. 将 `lake_autovacuum_grace_period_minutes` 设置为足够大的值：

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="10000000");
   ```

   :::warning
   此设置会阻止源集群回收迁移期间的过期对象存储文件，导致存储放大。请尽量缩短迁移窗口期。迁移完成后，务必将该值重置为默认值（`30`）。
   :::

2. 迁移完成后，重置该配置：

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

### 目标集群配置

以下配置需要在数据迁移前在目标集群上完成。

#### 开放端口

如果您开启了防火墙，需开通以下 FE 端口：

| **组件** | **端口**   | **默认值** |
| -------- | ---------- | ---------- |
| FE       | query_port | 9030 |
| FE       | http_port  | 8030 |
| FE       | rpc_port   | 9020 |

:::note
由于数据通过对象存储系统直接传输，您**无需**开放源集群的 CN 或 BE 端口。
:::

#### 关闭 Compaction

迁移期间需关闭目标集群的 Compaction，以避免与数据迁移产生冲突。

1. 动态关闭 Compaction：

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="0");
   ```

2. 为防止集群重启后 Compaction 被重新开启，还需在 FE 配置文件 **fe.conf** 中添加以下配置：

   ```Properties
   lake_compaction_max_tasks = 0
   ```

迁移完成后，从 **fe.conf** 中删除该配置，并执行以下语句重新开启 Compaction：

```SQL
ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
```

#### 开启迁移旧版本兼容

新旧版本的集群间可能存在行为差异，导致跨集群数据迁移时出现问题。因此在数据迁移前，您需要为目标集群开启旧版本兼容，并在数据迁移完成后关闭。

1. 通过以下语句查看当前集群是否已开启旧版本兼容：

   ```SQL
   ADMIN SHOW FRONTEND CONFIG LIKE 'enable_legacy_compatibility_for_replication';
   ```

   如果返回值为 `true`，则表示已开启。

2. 动态开启旧版本兼容：

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="true");
   ```

3. 为防止集群重启后该设置失效，还需在 **fe.conf** 中添加以下配置：

   ```Properties
   enable_legacy_compatibility_for_replication = true
   ```

数据迁移完成后，从 **fe.conf** 中删除该配置，并执行以下语句关闭旧版本兼容：

```SQL
ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
```

#### 在目标集群创建源存储卷

迁移工具会识别源集群每张表所使用的存储卷，并在目标集群上按照 `src_<源存储卷名称>` 的命名规则查找对应的存储卷。您需要在迁移开始前预先创建这些存储卷。

1. 在**源集群**上查看所有存储卷：

   ```SQL
   SHOW STORAGE VOLUMES;
   ```

2. 针对需要迁移的表所使用的每个存储卷，获取其详细配置：

   ```SQL
   DESCRIBE STORAGE VOLUME <volume_name>;
   ```

   示例输出：

   ```
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | Name                | Type | IsDefault | Location                      | Params                   |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | builtin_storage_vol | S3   | true      | s3://my-bucket                | {"aws.s3.region":"...",...} |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   ```

3. 在**目标集群**上，使用相同的对象存储凭证创建对应的存储卷，命名时加上 `src_` 前缀：

   ```SQL
   CREATE STORAGE VOLUME src_<源存储卷名称>
   TYPE = S3
   LOCATIONS = ("<与源集群相同的存储路径>")
   PROPERTIES
   (
       "enabled" = "true",
       "aws.s3.region" = "<region>",
       "aws.s3.endpoint" = "<endpoint>",
       "aws.s3.use_aws_sdk_default_behavior" = "false",
       "aws.s3.use_instance_profile" = "false",
       "aws.s3.access_key" = "<access_key>",
       "aws.s3.secret_key" = "<secret_key>",
       "aws.s3.enable_partitioned_prefix" = "false"
   );
   ```

   :::note
   - 无论源集群是否开启了分区前缀（partitioned prefix），目标集群上的镜像存储卷均需将 `aws.s3.enable_partitioned_prefix` 设置为 `false`。迁移工具会直接使用源分区的完整路径读取文件，不需要再次应用分区前缀。
   - 对于每个需要迁移的表所使用的**不同存储卷**，都需要重复此步骤。例如，若源集群使用了 `builtin_storage_volume`，则需要在目标集群上创建 `src_builtin_storage_volume`。
   - 建议为源存储卷使用临时凭证（Access Key / Secret Key），迁移完成后可及时吊销。
   :::

#### 配置迁移参数（可选）

您可以通过以下 FE 和 BE 参数配置迁移操作。大多数情况下，默认配置即可满足需求。

:::note
提高以下配置值可加速迁移，但同时会增加源集群的负载压力。
:::

**FE 参数**（动态配置，无需重启）：

| **参数**                              | **默认值** | **单位** | **说明**                                                     |
| ------------------------------------- | ---------- | -------- | ------------------------------------------------------------ |
| replication_max_parallel_table_count  | 100        | -        | 允许并发的数据同步任务最大数量。迁移工具为每张表创建一个同步任务。 |
| replication_max_parallel_replica_count| 10240      | -        | 允许并发同步的 Tablet 副本最大数量。 |
| replication_max_parallel_data_size_mb | 1048576    | MB       | 允许并发同步的最大数据量。 |
| replication_transaction_timeout_sec   | 86400      | 秒       | 同步任务的超时时间。 |

**BE/CN 参数**（动态配置，无需重启）：

| **参数**            | **默认值** | **单位** | **说明**                                                     |
| ------------------- | ---------- | -------- | ------------------------------------------------------------ |
| replication_threads | 0          | -        | 执行同步任务的线程数。`0` 表示设置为机器 CPU 核数的 4 倍。 |

## 第一步：安装迁移工具

建议将迁移工具安装在目标集群所在的服务器上。

1. 下载二进制包：

   ```Bash
   wget https://releases.starrocks.io/starrocks/starrocks-cluster-sync.tar.gz
   ```

2. 解压：

   ```Bash
   tar -xvzf starrocks-cluster-sync.tar.gz
   ```

## 第二步：配置迁移工具

### 配置迁移参数

进入解压目录，修改配置文件 **conf/sync.properties**：

```Bash
cd starrocks-cluster-sync
vi conf/sync.properties
```

文件内容如下：

```Properties
# If true, all tables will be synchronized only once, and the program will exit automatically after completion.
one_time_run_mode=false

source_fe_host=
source_fe_query_port=9030
source_cluster_user=root
source_cluster_password=
source_cluster_password_secret_key=
# source_cluster_token 对于存算分离源集群不是必填项，留空即可。
source_cluster_token=

target_fe_host=
target_fe_query_port=9030
target_cluster_user=root
target_cluster_password=
target_cluster_password_secret_key=

jdbc_connect_timeout_ms=30000
jdbc_socket_timeout_ms=60000

# Comma-separated list of database names or table names like <db_name> or <db_name.table_name>
# example: db1,db2.tbl2,db3
# Effective order: 1. include 2. exclude
include_data_list=
exclude_data_list=

# If there are no special requirements, please maintain the default values for the following configurations.
target_cluster_storage_volume=
target_cluster_replication_num=-1
target_cluster_max_disk_used_percent=80
# To maintain consistency with the source cluster, use null.
target_cluster_enable_persistent_index=
# Whether to use builtin_storage_volume on the target cluster.
# When set to true, tables created on the target cluster will use builtin_storage_volume uniformly,
# instead of using the source cluster's storage_volume configuration.
target_cluster_use_builtin_storage_volume_only=false

max_replication_data_size_per_job_in_gb=1024

meta_job_interval_seconds=180
meta_job_threads=4
ddl_job_interval_seconds=5
ddl_job_batch_size=10

# table config
ddl_job_allow_drop_target_only=false
ddl_job_allow_drop_schema_change_table=true
ddl_job_allow_drop_inconsistent_partition=true
ddl_job_allow_drop_inconsistent_time_partition = true
ddl_job_allow_drop_partition_target_only=true
# index config
enable_bitmap_index_sync=false
ddl_job_allow_drop_inconsistent_bitmap_index=true
ddl_job_allow_drop_bitmap_index_target_only=true
# MV config
enable_materialized_view_sync=false
ddl_job_allow_drop_inconsistent_materialized_view=true
ddl_job_allow_drop_materialized_view_target_only=false
# View config
enable_view_sync=false
ddl_job_allow_drop_inconsistent_view=true
ddl_job_allow_drop_view_target_only=false

replication_job_interval_seconds=10
replication_job_batch_size=10
report_interval_seconds=300

enable_table_property_sync=false
```

参数说明如下：

| **参数**                                    | **说明**                                                     |
| ------------------------------------------- | ------------------------------------------------------------ |
| one_time_run_mode                           | 是否启用一次性同步模式。开启后，迁移工具只执行全量同步，完成后自动退出。 |
| source_fe_host                              | 源集群 FE 的 IP 地址或 FQDN。 |
| source_fe_query_port                        | 源集群 FE 的查询端口（`query_port`）。 |
| source_cluster_user                         | 登录源集群的用户名。该用户需具备 SYSTEM 级别的 OPERATE 权限。 |
| source_cluster_password                     | 登录源集群的用户密码。 |
| source_cluster_password_secret_key          | 用于加密 `source_cluster_password` 的密钥。默认为空字符串（不加密）。如需加密，可通过 SQL 语句 `SELECT TO_BASE64(AES_ENCRYPT('<password>','<secret_key>'))` 获取加密后的密码字符串。 |
| source_cluster_token                        | 源集群的 Token。**存算分离源集群无需填写，留空即可。** Cluster Token 仅用于存算一体源集群场景下 BE 间快照传输的认证；存算分离场景下，数据直接通过对象存储读取，不经过 BE，因此无需 Token。 |
| target_fe_host                              | 目标集群 FE 的 IP 地址或 FQDN。 |
| target_fe_query_port                        | 目标集群 FE 的查询端口（`query_port`）。 |
| target_cluster_user                         | 登录目标集群的用户名。该用户需具备 SYSTEM 级别的 OPERATE 权限。 |
| target_cluster_password                     | 登录目标集群的用户密码。 |
| target_cluster_password_secret_key          | 用于加密 `target_cluster_password` 的密钥。加密机制与 `source_cluster_password_secret_key` 相同。 |
| jdbc_connect_timeout_ms                     | 连接 FE 进行 SQL 查询时的 JDBC 连接超时时间（毫秒）。默认值：`30000`。 |
| jdbc_socket_timeout_ms                      | 连接 FE 进行 SQL 查询时的 JDBC Socket 超时时间（毫秒）。默认值：`60000`。 |
| include_data_list                           | 需要迁移的数据库和表，多个对象用逗号（`,`）分隔。例如，`db1,db2.tbl2,db3`。该参数优先于 `exclude_data_list` 生效。如需迁移全部数据库和表，无需配置该项。 |
| exclude_data_list                           | 不需要迁移的数据库和表，多个对象用逗号（`,`）分隔。`include_data_list` 优先于该参数生效。如需迁移全部数据库和表，无需配置该项。 |
| target_cluster_storage_volume               | 目标集群上新建表所使用的存储卷。若为空，则使用目标集群的默认存储卷。该参数与源存储卷（`src_<name>` 存储卷）相互独立。 |
| target_cluster_replication_num              | 在目标集群上建表时指定的副本数。`-1` 表示与源集群保持一致。 |
| target_cluster_max_disk_used_percent        | （仅适用于存算一体目标集群）目标集群 BE 节点的磁盘使用率上限。当任意 BE 超过该值时，迁移停止。默认值：`80`（80%）。 |
| target_cluster_enable_persistent_index      | 是否在目标集群开启持久化索引。为空（默认）时与源集群保持一致。 |
| target_cluster_use_builtin_storage_volume_only | 设为 `true` 时，目标集群上所有表统一使用 `builtin_storage_volume`，忽略源集群的存储卷配置。适用于源集群有多个自定义存储卷，但希望在目标集群统一使用单一存储卷的场景。默认值：`false`。 |
| meta_job_interval_seconds                   | 迁移工具从源、目标集群获取元数据的间隔时间（秒）。 |
| meta_job_threads                            | 迁移工具获取元数据的线程数。 |
| ddl_job_interval_seconds                    | 迁移工具在目标集群执行 DDL 语句的间隔时间（秒）。 |
| ddl_job_batch_size                          | 在目标集群执行 DDL 语句的批处理大小。 |
| ddl_job_allow_drop_target_only              | 是否允许迁移工具删除仅存在于目标集群而不存在于源集群的数据库或表。默认值：`false`（不删除）。 |
| ddl_job_allow_drop_schema_change_table      | 是否允许迁移工具删除源、目标集群之间 Schema 不一致的表。默认值：`true`（删除）。迁移工具会在迁移过程中自动重新同步被删除的表。 |
| ddl_job_allow_drop_inconsistent_partition   | 是否允许迁移工具删除源、目标集群之间数据分布不一致的分区。默认值：`true`（删除）。迁移工具会在迁移过程中自动重新同步被删除的分区。 |
| ddl_job_allow_drop_partition_target_only    | 是否允许迁移工具删除在源集群中已被删除的分区，以保持两端分区的一致性。默认值：`true`（删除）。 |
| replication_job_interval_seconds            | 迁移工具触发数据同步任务的间隔时间（秒）。 |
| replication_job_batch_size                  | 迁移工具触发数据同步任务的批处理大小。 |
| max_replication_data_size_per_job_in_gb     | 触发数据同步任务的数据量阈值（GB）。超过此值时，迁移工具会拆分为多个同步任务。默认值：`1024`。 |
| report_interval_seconds                     | 迁移工具打印进度信息的间隔时间（秒）。默认值：`300`。 |
| enable_bitmap_index_sync                    | 是否同步 Bitmap 索引。默认值：`false`。 |
| ddl_job_allow_drop_inconsistent_bitmap_index | 是否允许迁移工具删除源、目标集群之间不一致的 Bitmap 索引。默认值：`true`（删除）。 |
| ddl_job_allow_drop_bitmap_index_target_only | 是否允许迁移工具删除在源集群中已删除的 Bitmap 索引，以保持两端一致性。默认值：`true`（删除）。 |
| enable_materialized_view_sync               | 是否同步物化视图。默认值：`false`。 |
| ddl_job_allow_drop_inconsistent_materialized_view | 是否允许迁移工具删除源、目标集群之间不一致的物化视图。默认值：`true`（删除）。 |
| ddl_job_allow_drop_materialized_view_target_only | 是否允许迁移工具删除在源集群中已删除的物化视图，以保持两端一致性。默认值：`false`（不删除）。 |
| enable_view_sync                            | 是否同步逻辑视图。默认值：`false`。 |
| ddl_job_allow_drop_inconsistent_view        | 是否允许迁移工具删除源、目标集群之间不一致的逻辑视图。默认值：`true`（删除）。 |
| ddl_job_allow_drop_view_target_only         | 是否允许迁移工具删除在源集群中已删除的逻辑视图，以保持两端一致性。默认值：`false`（不删除）。 |
| enable_table_property_sync                  | 是否同步表属性。默认值：`false`。 |

:::note
**主键表持久化索引**：在两个存算分离集群之间迁移时，迁移工具会自动将主键表建表语句中的 `persistent_index_type = LOCAL` 改写为 `CLOUD_NATIVE`，无需手动操作。
:::

### 存储卷映射说明

迁移工具在目标集群创建表时，按以下优先级决定表所使用的存储卷：

1. 若 `target_cluster_use_builtin_storage_volume_only = true`：所有表统一使用 `builtin_storage_volume`。
2. 若 `target_cluster_storage_volume = <名称>`：所有表使用指定的存储卷。
3. 以上均未配置（默认）：保留源集群表的存储卷名称。源集群不同存储卷上的表，将在目标集群上分别绑定到同名存储卷（前提是这些存储卷已在目标集群上创建）。

因此，**目标集群支持创建多个非 `src_` 前缀的存储卷**。例如，若源集群的表分布在 `ssd_volume` 和 `oss_volume` 两个存储卷上，则可以在目标集群上预先创建这两个存储卷，迁移完成后各表将分别绑定到对应的存储卷。

`src_<名称>` 存储卷的用途与此不同：它们**仅在迁移过程中使用**，用于授权目标集群的 CN 读取源集群对象存储中的数据文件。迁移完成后，`src_<名称>` 存储卷不再需要，可以删除。

### 配置网络（可选）

迁移过程中，迁移工具需要访问源集群和目标集群的**所有** FE 节点。

:::note
与存算一体到存算分离的迁移不同，您**无需**配置目标集群到源集群 CN 节点的网络访问，因为数据是直接通过对象存储系统传输的。
:::

您可以通过在对应集群上执行以下语句获取 FE 节点的网络地址：

```SQL
-- 获取 FE 节点地址
SHOW FRONTENDS;
```

如果 FE 节点使用私有地址（例如 Kubernetes 集群内部地址），无法从外部访问，则需要在 **conf/hosts.properties** 中配置地址映射：

```Bash
cd starrocks-cluster-sync
vi conf/hosts.properties
```

格式如下：

```Properties
# <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
```

:::note
`<host>` 必须与 `SHOW FRONTENDS` 返回结果中 `IP` 列的地址完全一致。
:::

示例：将目标集群的 Kubernetes 内部 FQDN 映射为可访问的 IP 地址：

```Properties
TARGET_frontend-0.frontend.mynamespace.svc.cluster.local=10.1.2.1;9030:19030
```

## 第三步：启动迁移工具

完成配置后，启动迁移工具开始数据迁移：

```Bash
./bin/start.sh
```

:::note

- 迁移工具会定期检查目标集群的数据是否落后于源集群，若发现落后则自动启动新的数据同步任务。
- 若源集群在迁移过程中不断有新数据写入，数据同步将持续进行，直到目标集群数据与源集群完全一致。
- 迁移过程中可以查询目标集群中的表，但请勿向目标集群写入新数据，否则可能导致源、目标集群数据不一致。
- 迁移工具不会自动停止。需要手动确认迁移完成后再停止工具。

:::

## 查看迁移进度

### 查看迁移工具日志

通过迁移工具日志 **log/sync.INFO.log** 查看迁移进度。

**查看任务进度**（搜索关键字 `Sync job progress`）：

![img](../_assets/data_migration_tool-1.png)

关键指标说明：

| **指标**    | **说明** |
| ----------- | -------- |
| `total`     | 此次数据迁移中的总作业数。 |
| `ddlPending`| 待执行的 DDL 作业数量。 |
| `jobPending`| 待执行的数据同步作业数量。 |
| `sent`      | 已发送但尚未开始的数据同步作业数量。若该值持续增大，请联系工程师排查。 |
| `running`   | 正在运行中的数据同步作业数量。 |
| `finished`  | 已完成的数据同步作业数量。 |
| `failed`    | 失败的数据同步作业累计数量。失败的作业会自动重试，通常可忽略。若该值持续显著增大，请排查原因。 |
| `unknown`   | 状态未知的作业数量。理论上应始终为 `0`。 |

`Sync job progress` 为 100% 表示当前检查周期内的数据同步已完成。若源集群持续有新数据写入，进度可能在下一个周期内下降，这属于正常现象。

**查看表迁移进度**（搜索关键字 `Sync table progress`）：

![img](../_assets/data_migration_tool-2.png)

| **指标**             | **说明** |
| -------------------- | -------- |
| `finishedTableRatio` | 至少有一次成功同步任务的数据表占比。 |
| `expiredTableRatio`  | 数据表过期数据占比。 |
| `total table`        | 此次数据迁移涉及的数据表总数。 |
| `finished table`     | 至少有一次成功同步任务的数据表数量。 |
| `unfinished table`   | 尚未进行过数据同步的数据表数量。 |
| `expired table`      | 存在过期数据的数据表数量。 |

### 查看迁移事务状态

迁移工具会为每张表开启一个事务，通过查看事务状态了解迁移进度：

```SQL
SHOW PROC "/transactions/<db_name>/running";
```

其中，`<db_name>` 为该表所在数据库的名称。

### 查看分区数据版本

通过对比源、目标集群相应分区的数据版本，了解该分区的迁移状态：

```SQL
SHOW PARTITIONS FROM <table_name>;
```

### 查看数据量

```SQL
SHOW DATA;
```

### 查看表行数

```SQL
SELECT
  TABLE_NAME,
  TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

## 迁移完成后的操作

当 `Sync job progress` 持续稳定在 100%，且业务处于可切换窗口期时，请按以下步骤完成切换：

1. 停止向源集群写入新数据。
2. 再次确认 `Sync job progress` 在停止写入后达到并保持 100%。
3. 停止迁移工具。
4. 将应用程序的数据源地址从源集群切换至目标集群，并启动业务。
5. 重置源集群的自动 Vacuum 配置：

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

6. 重新开启目标集群的 Compaction。从 **fe.conf** 中删除 `lake_compaction_max_tasks = 0`，然后执行：

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
   ```

7. 关闭目标集群的旧版本兼容。从 **fe.conf** 中删除 `enable_legacy_compatibility_for_replication = true`，然后执行：

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
   ```

## 限制

当前支持同步的对象类型如下（不在此列表中的对象不支持同步）：

- 数据库
- 内表及其数据
- 物化视图的 Schema 及其构建语句（物化视图的数据不会同步；若物化视图的基表未迁移到目标集群，后台刷新任务将报错）
- 逻辑视图

存算分离到存算分离迁移的额外限制：

- 目标集群必须为存算分离集群（v4.1 及以上）。不支持迁移到存算一体目标集群。
- 源集群每张表所使用的存储卷，必须在目标集群上预先创建对应的 `src_<存储卷名称>` 存储卷。
