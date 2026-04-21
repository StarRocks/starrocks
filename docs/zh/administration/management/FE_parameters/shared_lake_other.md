---
displayed_sidebar: docs
sidebar_label: "存算分离、数据湖和其他"
---

# FE 配置 - 存算分离、数据湖和其他

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## 查看 FE 配置项

FE 启动后，您可以在 MySQL 客户端运行 ADMIN SHOW FRONTEND CONFIG 命令查看参数配置。如果要查询特定参数的配置，请运行以下命令：

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

有关返回字段的详细说明，请参阅 [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md)。

:::note
您必须具有管理员权限才能运行集群管理相关命令。
:::

## 配置 FE 参数

### 配置 FE 动态参数

您可以使用 [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) 命令配置或修改 FE 动态参数。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### 配置 FE 静态参数

<StaticFEConfigNote />

---

当前主题包含以下类型的 FE 配置：
- [存算分离](#存算分离)
- [数据湖](#数据湖)
- [其他](#其他)

## 存算分离

### `aws_s3_access_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 Access Key ID。
- 引入版本: v3.0

### `aws_s3_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 endpoint，例如 `https://s3.us-west-2.amazonaws.com`。
- 引入版本: v3.0

### `aws_s3_external_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于跨账户访问 S3 存储桶的 AWS 账户的外部 ID。
- 引入版本: v3.0

### `aws_s3_iam_role_arn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 对存储数据文件的 S3 存储桶具有权限的 IAM 角色的 ARN。
- 引入版本: v3.0

### `aws_s3_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 S3 路径。它由 S3 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本: v3.0

### `aws_s3_region`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: S3 存储桶所在的区域，例如 `us-west-2`。
- 引入版本: v3.0

### `aws_s3_secret_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于访问 S3 存储桶的 Secret Access Key。
- 引入版本: v3.0

### `aws_s3_use_aws_sdk_default_behavior`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用 AWS SDK 的默认认证凭据。有效值：true 和 false (默认)。
- 引入版本: v3.0

### `aws_s3_use_instance_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用实例配置文件和 assumed role 作为访问 S3 的凭证方法。有效值：true 和 false (默认)。
  - 如果您使用基于 IAM 用户（Access Key 和 Secret Key）的凭证访问 S3，则必须将此项指定为 `false`，并指定 `aws_s3_access_key` 和 `aws_s3_secret_key`。
  - 如果您使用实例配置文件访问 S3，则必须将此项指定为 `true`。
  - 如果您使用 assumed role 访问 S3，则必须将此项指定为 `true`，并指定 `aws_s3_iam_role_arn`。
  - 如果您使用外部 AWS 账户，则还必须指定 `aws_s3_external_id`。
- 引入版本: v3.0

### `azure_adls2_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Azure Data Lake Storage Gen2 账户的 endpoint，例如 `https://test.dfs.core.windows.net`。
- 引入版本: v3.4.1

### `azure_adls2_oauth2_client_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的托管标识的客户端 ID。
- 引入版本: v3.4.4

### `azure_adls2_oauth2_tenant_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的托管标识的租户 ID。
- 引入版本: v3.4.4

### `azure_adls2_oauth2_use_managed_identity`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用托管标识授权 Azure Data Lake Storage Gen2 请求。
- 引入版本: v3.4.4

### `azure_adls2_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Azure Data Lake Storage Gen2 路径。它由文件系统名称和目录名称组成，例如 `testfilesystem/starrocks`。
- 引入版本: v3.4.1

### `azure_adls2_sas_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的共享访问签名 (SAS)。
- 引入版本: v3.4.1

### `azure_adls2_shared_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Data Lake Storage Gen2 请求的共享密钥。
- 引入版本: v3.4.1

### `azure_blob_endpoint`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: Azure Blob Storage 账户的 endpoint，例如 `https://test.blob.core.windows.net`。
- 引入版本: v3.1

### `azure_blob_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Azure Blob Storage 路径。它由存储账户中容器的名称和容器下的子路径（如果有）组成，例如 `testcontainer/subpath`。
- 引入版本: v3.1

### `azure_blob_sas_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Blob Storage 请求的共享访问签名 (SAS)。
- 引入版本: v3.1

### `azure_blob_shared_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 Azure Blob Storage 请求的共享密钥。
- 引入版本: v3.1

### `azure_use_native_sdk`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否使用原生 SDK 访问 Azure Blob Storage，从而允许使用托管标识和服务主体进行身份验证。如果此项设置为 `false`，则仅允许使用共享密钥和 SAS Token 进行身份验证。
- 引入版本: v3.4.4

### `cloud_native_hdfs_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: HDFS 存储的 URL，例如 `hdfs://127.0.0.1:9000/user/xxx/starrocks/`。
- 引入版本: -

### `cloud_native_meta_port`

- 默认值: 6090
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: FE 云原生元数据服务器 RPC 监听端口。
- 引入版本: -

### `cloud_native_storage_type`

- 默认值: S3
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 您使用的对象存储类型。在共享数据模式下，StarRocks 支持将数据存储在 HDFS、Azure Blob（v3.1.1 起支持）、Azure Data Lake Storage Gen2（v3.4.1 起支持）、Google Storage（带原生 SDK，v3.5.1 起支持）以及与 S3 协议兼容的对象存储系统（如 AWS S3 和 MinIO）中。有效值：`S3`（默认）、`HDFS`、`AZBLOB`、`ADLS2` 和 `GS`。如果您将此参数指定为 `S3`，则必须添加以 `aws_s3` 为前缀的参数。如果您将此参数指定为 `AZBLOB`，则必须添加以 `azure_blob` 为前缀的参数。如果您将此参数指定为 `ADLS2`，则必须添加以 `azure_adls2` 为前缀的参数。如果您将此参数指定为 `GS`，则必须添加以 `gcp_gcs` 为前缀的参数。如果您将此参数指定为 `HDFS`，则只需指定 `cloud_native_hdfs_url`。
- 引入版本: -

### `enable_load_volume_from_conf`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否允许 StarRocks 使用 FE 配置文件中指定的对象存储相关属性创建内置存储卷。从 v3.4.1 开始，默认值从 `true` 更改为 `false`。
- 引入版本: v3.1.0

### `gcp_gcs_impersonation_service_account`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 如果您使用基于模拟的身份验证访问 Google Storage，则要模拟的服务账户。
- 引入版本: v3.5.1

### `gcp_gcs_path`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于存储数据的 Google Cloud 路径。它由 Google Cloud 存储桶的名称和其下的子路径（如果有）组成，例如 `testbucket/subpath`。
- 引入版本: v3.5.1

### `gcp_gcs_service_account_email`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的电子邮件地址，例如 `user@hello.iam.gserviceaccount.com`。
- 引入版本: v3.5.1

### `gcp_gcs_service_account_private_key`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的私钥，例如 `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`。
- 引入版本: v3.5.1

### `gcp_gcs_service_account_private_key_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 服务账户创建时生成的 JSON 文件中的私钥 ID。
- 引入版本: v3.5.1

### `gcp_gcs_use_compute_engine_service_account`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 是否使用绑定到 Compute Engine 的服务账户。
- 引入版本: v3.5.1

### `hdfs_file_system_expire_seconds`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 由 HdfsFsManager 管理的未使用的缓存 HDFS/ObjectStore FileSystem 的存活时间（秒）。FileSystemExpirationChecker（每 60 秒运行一次）使用此值调用每个 HdfsFs.isExpired(...)；过期时，管理器关闭底层 FileSystem 并将其从缓存中删除。访问器方法（例如 `HdfsFs.getDFSFileSystem`、`getUserName`、`getConfiguration`）更新最后访问时间戳，因此过期基于不活动。较低的值会减少空闲资源占用，但会增加重新打开的开销；较高的值会保持句柄更长时间，并可能消耗更多资源。
- 引入版本: v3.2.0

### `lake_autovacuum_grace_period_minutes`

- 默认值: 30
- 类型: Long
- 单位: 分钟
- 是否可变: Yes
- 描述: 共享数据集群中保留历史数据版本的时间范围。在此时间范围内的历史数据版本不会在 Compactions 后通过 AutoVacuum 自动清理。您需要将此值设置得大于最大查询时间，以避免正在运行的查询访问的数据在查询完成之前被删除。从 v3.3.0、v3.2.5 和 v3.1.10 开始，默认值已从 `5` 更改为 `30`。
- 引入版本: v3.1.0

### `lake_autovacuum_parallel_partitions`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 共享数据集群中可同时进行 AutoVacuum 的分区最大数量。AutoVacuum 是 Compactions 后的垃圾回收。
- 引入版本: v3.1.0

### `lake_autovacuum_partition_naptime_seconds`

- 默认值: 180
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: 共享数据集群中同一分区两次 AutoVacuum 操作之间的最小间隔。
- 引入版本: v3.1.0

### `lake_autovacuum_stale_partition_threshold`

- 默认值: 12
- 类型: Long
- 单位: 小时
- 是否可变: Yes
- 描述: 如果分区在此时间范围内没有更新（加载、DELETE 或 Compactions），系统将不会对此分区执行 AutoVacuum。
- 引入版本: v3.1.0

### `lake_compaction_allow_partial_success`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果此项设置为 `true`，则在共享数据集群中，当子任务之一成功时，系统将认为 Compaction 操作成功。
- 引入版本: v3.5.2

### `lake_compaction_disable_ids`

- 默认值: ""
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据模式下禁用 Compaction 的表或分区列表。格式为 `tableId1;partitionId2`，以分号分隔，例如 `12345;98765`。
- 引入版本: v3.4.4

### `lake_compaction_history_size`

- 默认值: 20
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据集群中，Leader FE 内存中保留的最近成功 Compaction 任务记录数。您可以使用 `SHOW PROC '/compactions'` 命令查看最近成功的 Compaction 任务记录。请注意，Compaction 历史记录存储在 FE 进程内存中，如果 FE 进程重启，它将丢失。
- 引入版本: v3.1.0

### `lake_compaction_max_parallel_default`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 当建表时未指定 `lake_compaction_max_parallel` 表属性时，每个 tablet 的默认最大并行 Compaction 子任务数。`0` 表示禁用并行 Compaction。此配置作为表属性 `lake_compaction_max_parallel` 的默认值。

### `lake_compaction_max_tasks`

- 默认值: -1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中允许的最大并发 Compaction 任务数。将此项设置为 `-1` 表示以自适应方式计算并发任务数。将此值设置为 `0` 将禁用 Compaction。
- 引入版本: v3.1.0

### `lake_compaction_score_selector_min_score`

- 默认值: 10.0
- 类型: Double
- 单位: -
- 是否可变: Yes
- 描述: 触发共享数据集群中 Compaction 操作的 Compaction Score 阈值。当分区的 Compaction Score 大于或等于此值时，系统会对该分区执行 Compaction。
- 引入版本: v3.1.0

### `lake_compaction_score_upper_bound`

- 默认值: 2000
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中分区的 Compaction Score 上限。`0` 表示无上限。此项仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。当分区的 Compaction Score 达到或超过此上限时，传入的加载任务将被拒绝。从 v3.3.6 开始，默认值从 `0` 更改为 `2000`。
- 引入版本: v3.2.0

### `lake_compaction_interval_ms_on_success`

- 默认值：10000
- 类型：Long
- 单位：毫秒
- 是否动态：是
- 描述：存算分离集群某个分区 Compaction 成功后，间隔多少时间，对这个分区发起下一次 Compaction。别名为 `lake_min_compaction_interval_ms_on_success`。
- 引入版本：v3.2.0

### `lake_enable_balance_tablets_between_workers`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在共享数据集群中，云原生表 tablet 迁移期间是否平衡计算节点之间的 tablet 数量。`true` 表示在计算节点之间平衡 tablet，`false` 表示禁用此功能。
- 引入版本: v3.3.4

### `lake_enable_ingest_slowdown`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在共享数据集群中启用数据摄取减速。当数据摄取减速启用时，如果分区的 Compaction Score 超过 `lake_ingest_slowdown_threshold`，则该分区上的加载任务将受到限制。此配置仅在 `run_mode` 设置为 `shared_data` 时生效。从 v3.3.6 开始，默认值从 `false` 更改为 `true`。
- 引入版本: v3.2.0

### `lake_ingest_slowdown_threshold`

- 默认值: 100
- 类型: Long
- 单位: -
- 是否可变: Yes
- 描述: 触发共享数据集群中数据摄取减速的 Compaction Score 阈值。此配置仅在 `lake_enable_ingest_slowdown` 设置为 `true` 时生效。
- 引入版本: v3.2.0

### `lake_publish_version_max_threads`

- 默认值: 512
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 共享数据集群中版本发布任务的最大线程数。
- 引入版本: v3.2.0

### `meta_sync_force_delete_shard_meta`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许直接删除共享数据集群的元数据，绕过清理远程存储文件。建议仅在存在大量待清理分片，导致 FE JVM 内存压力过大时才将此项设置为 `true`。请注意，启用此功能后，属于分片或 tablet 的数据文件无法自动清理。
- 引入版本: v3.2.10, v3.3.3

### `run_mode`

- 默认值: `shared_nothing`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 集群的运行模式。有效值：`shared_data` 和 `shared_nothing`（默认）。
  - `shared_data` 表示以共享数据模式运行 StarRocks。
  - `shared_nothing` 表示以共享无数据模式运行 StarRocks。

  > **CAUTION**
  >
  > - StarRocks 集群不能同时采用 `shared_data` 和 `shared_nothing` 模式。不支持混合部署。
  > - 集群部署后，请勿更改 `run_mode`。否则，集群将无法重启。不支持从共享无数据集群转换为共享数据集群，反之亦然。

- 引入版本: -

### `shard_group_clean_threshold_sec`

- 默认值: 3600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: FE 清理共享数据集群中未使用的 tablet 和 shard 组的时间。在此阈值内创建的 tablet 和 shard 组将不会被清理。
- 引入版本: -

### `star_mgr_meta_sync_interval_sec`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: FE 在共享数据集群中与 StarMgr 进行周期性元数据同步的间隔。
- 引入版本: -

### `starmgr_grpc_server_max_worker_threads`

- 默认值: 1024
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: FE starmgr 模块中 grpc 服务器使用的最大工作线程数。
- 引入版本: v4.0.0, v3.5.8

### `starmgr_grpc_timeout_seconds`

- 默认值: 5
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述:
- 引入版本: -

## 数据湖

### `files_enable_insert_push_down_column_type`

- 默认值: true
- 别名: `files_enable_insert_push_down_schema`
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，StarRocks 会将目标表的列类型下推到 `files()` 表函数，用于 `INSERT INTO target_table SELECT ... FROM files()` 操作。仅对 files 推断出的已有列进行类型重写，不添加或删除列。复杂类型会跳过。这可减少由文件类型推断不准确引起的类型不匹配错误。如需完整的 schema 下推（列名和类型），请使用 INSERT 属性 `enable_push_down_schema`。
- 引入版本: v3.4.0, v3.5.0

### `hdfs_read_buffer_size_kb`

- 默认值: 8192
- 类型: Int
- 单位: 千字节
- 是否可变: Yes
- 描述: HDFS 读取缓冲区的大小（以千字节为单位）。StarRocks 将此值转换为字节（`<< 10`），并用它来初始化 `HdfsFsManager` 中的 HDFS 读取缓冲区，并在不使用 broker 访问时填充发送给 BE 任务的 thrift 字段 `hdfs_read_buffer_size_kb`（例如 `TBrokerScanRangeParams`、`TDownloadReq`）。增加 `hdfs_read_buffer_size_kb` 可以提高顺序读取吞吐量并减少系统调用开销，但代价是每个流的内存使用量更高；减小它会减少内存占用，但可能会降低 I/O 效率。调整时请考虑工作负载（许多小流与少数大型顺序读取）。
- 引入版本: v3.2.0

### `hdfs_write_buffer_size_kb`

- 默认值: 1024
- 类型: Int
- 单位: 千字节
- 是否可变: Yes
- 描述: 设置用于直接写入 HDFS 或对象存储时（不使用 broker）的 HDFS 写入缓冲区大小（以 KB 为单位）。FE 将此值转换为字节（`<< 10`），并初始化 HdfsFsManager 中的本地写入缓冲区，并在 Thrift 请求中传播（例如 TUploadReq、TExportSink、sink options），以便后端/代理使用相同的缓冲区大小。增加此值可以提高大型顺序写入的吞吐量，但代价是每个写入器占用更多内存；减小此值可以减少每个流的内存使用量，并可能降低小型写入的延迟。与 `hdfs_read_buffer_size_kb` 一起调整，并考虑可用内存和并发写入器。
- 引入版本: v3.2.0

### `lake_enable_drop_tablet_cache`

- 默认值：`true`
- 类型：Boolean
- 单位：-
- 是否动态：是
- 描述：shared-data 模式下，在实际数据被删除前，清理 BE/CN 上的缓存。
- 引入版本：v4.0

### `lake_batch_publish_max_version_num`

- 默认值: 10
- 类型: Int
- 单位: 计数
- 是否可变: Yes
- 描述: 设置在为 lake（云原生）表构建发布批次时，可能分组的连续事务版本的上限。该值传递给事务图批处理例程（参见 getReadyToPublishTxnListBatch），并与 `lake_batch_publish_min_version_num` 一起确定 TransactionStateBatch 的候选范围大小。较大的值可以通过批处理更多提交来提高发布吞吐量，但会增加原子发布的范围（更长的可见性延迟和更大的回滚表面），并且当版本不连续时可能会在运行时受到限制。根据工作负载和可见性/延迟要求进行调整。
- 引入版本: v3.2.0

### `lake_batch_publish_min_version_num`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 设置构成 lake 表发布批次所需的最小连续事务版本数。DatabaseTransactionMgr.getReadyToPublishTxnListBatch 将此值与 `lake_batch_publish_max_version_num` 一起传递给 transactionGraph.getTxnsWithTxnDependencyBatch 以选择依赖事务。值为 `1` 允许单事务发布（不批处理）。值 `>1` 要求至少有相同数量的连续版本、单表、非复制事务可用；如果版本不连续，出现复制事务，或 schema 更改消耗版本，则批处理中止。增加此值可以通过分组提交来提高发布吞吐量，但可能会在等待足够连续的事务时延迟发布。
- 引入版本: v3.2.0

### `lake_enable_batch_publish_version`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，PublishVersionDaemon 会为同一个 Lake（共享数据）表/分区批处理就绪事务，并将其版本一起发布，而不是为每个事务单独发布。在 RunMode shared-data 中，守护进程调用 getReadyPublishTransactionsBatch() 并使用 publishVersionForLakeTableBatch(...) 执行分组发布操作（减少 RPC 并提高吞吐量）。禁用时，守护进程回退到通过 publishVersionForLakeTable(...) 进行的逐事务发布。实现通过内部集合协调进行中的工作，以避免在切换开关时重复发布，并且受 `lake_publish_version_max_threads` 的线程池大小影响。
- 引入版本: v3.2.0

### `lake_enable_tablet_creation_optimization`

- 默认值: false
- 类型: boolean
- 单位: -
- 是否可变: Yes
- 描述: 启用后，StarRocks 在共享数据模式下优化云原生表和物化视图的 tablet 创建，通过为物理分区下的所有 tablet 创建单个共享 tablet 元数据，而不是为每个 tablet 创建不同的元数据。这减少了表创建、rollup 和 schema 变更作业期间创建的 tablet 任务和元数据/文件数量。优化仅适用于云原生表/物化视图，并与 `file_bundling` 结合（后者重用相同的优化逻辑）。注意：schema 变更和 rollup 作业明确禁用使用 `file_bundling` 的表的优化，以避免使用相同名称的文件被覆盖。谨慎启用——它改变了创建的 tablet 元数据的粒度，并可能影响副本创建和文件命名行为。
- 引入版本: v3.3.1, v3.4.0, v3.5.0

### `lake_create_tablet_max_retries`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 存算分离模式下建表时 create tablet 任务失败后的最大重试次数。当 CN 不可达或宕机时，失败的任务会在其他存活的 CN 上重试。仅发送阶段的失败（RPC 错误、节点宕机）会触发重试；CN 返回的错误和超时不会重试。设置为 `0` 可禁用重试。
- 引入版本: v4.1

### `lake_use_combined_txn_log`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 当此项设置为 `true` 时，系统允许 Lake 表使用组合事务日志路径进行相关事务。仅适用于共享数据集群。
- 引入版本: v3.3.7, v3.4.0, v3.5.0

### `lake_repair_metadata_fetch_max_version_batch_size`

- 默认值：160
- 类型：Long
- 单位：-
- 是否动态：是
- 描述：存算分离集群中，Tablet 修复时获取 Tablet 元数据的版本扫描最大批次大小。批次大小从 5 开始，每次翻倍增长，直到达到此最大值。较大的值允许单次批量获取更多版本，通过跨版本文件存在性缓存提高修复效率。如果设置的值小于 5，运行时会自动调整为 5。
- 引入版本：v3.5.16, v4.0.9

### `enable_iceberg_commit_queue`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 Iceberg 表启用提交队列以避免并发提交冲突。Iceberg 使用乐观并发控制 (OCC) 进行元数据提交。当多个线程同时提交到同一表时，可能会发生冲突，并出现诸如“无法提交：基元数据位置与当前表元数据位置不同”之类的错误。启用后，每个 Iceberg 表都有自己的单线程执行器用于提交操作，确保对同一表的提交是序列化的，并防止 OCC 冲突。不同的表可以并发提交，从而保持整体吞吐量。这是一项系统级优化，旨在提高可靠性，应默认启用。如果禁用，并发提交可能会因乐观锁定冲突而失败。
- 引入版本: v4.1.0

### `iceberg_commit_queue_timeout_seconds`

- 默认值: 300
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 等待 Iceberg 提交操作完成的超时时间（秒）。当使用提交队列 (`enable_iceberg_commit_queue=true`) 时，每个提交操作必须在此超时时间内完成。如果提交时间超过此超时，它将被取消并引发错误。影响提交时间的因素包括：正在提交的数据文件数量、表的元数据大小、底层存储（例如 S3、HDFS）的性能。
- 引入版本: v4.1.0

### `iceberg_commit_queue_max_size`

- 默认值: 1000
- 类型: Int
- 单位: 计数
- 是否可变: No
- 描述: 每个 Iceberg 表待处理提交操作的最大数量。当使用提交队列 (`enable_iceberg_commit_queue=true`) 时，这限制了可以为一个表排队的提交操作的数量。当达到限制时，额外的提交操作将在调用者线程中执行（阻塞直到容量可用）。此配置在 FE 启动时读取，并应用于新创建的表执行器。需要重启 FE 才能生效。如果您预期对同一表有许多并发提交，请增加此值。如果此值过低，在高并发期间提交可能会在调用者线程中阻塞。
- 引入版本: v4.1.0

### lake_balance_tablets_threshold

- 默认值：0.15
- 类型：Double
- 单位：-
- 是否可变：Yes
- 描述：系统用于判断存算分离集群中 Worker 之间 Tablet 分布平衡的阈值，不平衡因子的计算公式为 `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`。如果该因子大于 `lake_balance_tablets_threshold`，则会触发节点间 Tablet 调度。此配置项仅在 `lake_enable_balance_tablets_between_workers` 设为 `true`时生效。
- 引入版本：v3.3.4

## 其他

### `agent_task_resend_wait_time_ms`

- 默认值: 5000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: FE 在重新发送 agent 任务之前必须等待的持续时间。仅当任务创建时间与当前时间之间的间隔超过此参数的值时，才能重新发送 agent 任务。此参数用于防止重复发送 agent 任务。
- 引入版本: -

### `allow_system_reserved_names`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许用户创建以 `__op` 和 `__row` 开头的列名。要启用此功能，请将此参数设置为 `TRUE`。请注意，这些名称格式在 StarRocks 中保留用于特殊目的，创建此类列可能导致未定义的行为。因此，此功能默认禁用。
- 引入版本: v3.2.0

### `auth_token`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于 StarRocks 集群内部身份验证的令牌。如果未指定此参数，StarRocks 会在集群 Leader FE 首次启动时为集群生成一个随机令牌。
- 引入版本: -

### `authentication_ldap_simple_bind_base_dn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器开始搜索用户身份验证信息的基准 DN。
- 引入版本: -

### `authentication_ldap_simple_bind_root_dn`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 用于搜索用户身份验证信息的管理员 DN。
- 引入版本: -

### `authentication_ldap_simple_bind_root_pwd`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 用于搜索用户身份验证信息的管理员密码。
- 引入版本: -

### `authentication_ldap_simple_server_host`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器运行的主机。
- 引入版本: -

### `authentication_ldap_simple_server_port`

- 默认值: 389
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: LDAP 服务器的端口。
- 引入版本: -

### `authentication_ldap_simple_user_search_attr`

- 默认值: uid
- 类型: String
- 单位: -
- 是否可变: Yes
- 描述: 在 LDAP 对象中标识用户的属性名称。
- 引入版本: -

### `backup_job_default_timeout_ms`

- 默认值: 86400 * 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 备份作业的超时时长。如果超过此值，备份作业将失败。
- 引入版本: -

### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在 `SHOW PROC /BACKENDS/{id}` 命令中启用收集每个磁盘的 tablet 数量。
- 引入版本: v4.0.1, v3.5.8

### `enable_colocate_restore`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否为 Colocate 表启用备份和恢复。`true` 表示为 Colocate 表启用备份和恢复，`false` 表示禁用。
- 引入版本: v3.2.10, v3.3.3

### `enable_external_catalog_information_schema_tables_access_full_metadata`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 控制在构建 `information_schema.tables` 时，是否为外部 Catalog（如 Hive、Iceberg、JDBC）中的表加载完整元数据。当该项为 `false`（默认）时不会访问远端 metastore，因此外部表在 `information_schema.tables` 中的 `TABLE_COMMENT` 等字段可能为空，但查询开销较小且不会对外部服务产生额外请求。当该项为 `true` 时，FE 会访问对应的外部元数据服务，填充 `TABLE_COMMENT` 等字段，但代价是每张外部表都会产生额外的远端调用和一定延迟。
- 引入版本: -

### `enable_materialized_view_concurrent_prepare`

- 默认值: true
- 类型: Boolean
- 单位:
- 是否可变: Yes
- 描述: 是否并发准备物化视图以提高性能。
- 引入版本: v3.4.4

### `enable_metric_calculator`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: No
- 描述: 指定是否启用用于定期收集指标的功能。有效值：`TRUE` 和 `FALSE`。`TRUE` 指定启用此功能，`FALSE` 指定禁用此功能。
- 引入版本: -

### `enable_table_metrics_collect`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 中是否导出表级别指标。禁用后，FE 将跳过导出表指标（如表扫描/加载计数器和表大小指标），但仍将计数器记录在内存中。
- 引入版本: -

### `enable_mv_post_image_reload_cache`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: FE 加载镜像后是否执行重新加载标志检查。如果对一个基本物化视图执行检查，则对于其他与之相关的物化视图则不需要。
- 引入版本: v3.5.0

### `enable_mv_query_context_cache`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用查询级别物化视图重写缓存以提高查询重写性能。
- 引入版本: v3.3

### `enable_mv_refresh_collect_profile`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否默认对所有物化视图在刷新时启用 profile。
- 引入版本: v3.3.0

### `enable_mv_refresh_extra_prefix_logging`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在日志中启用带有物化视图名称的前缀，以便更好地调试。
- 引入版本: v3.4.0

### `enable_mv_refresh_query_rewrite`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否在物化视图刷新期间启用重写查询，以便查询可以直接使用重写的物化视图而不是基表来提高查询性能。
- 引入版本: v3.3

### `enable_trace_historical_node`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许系统跟踪历史节点。通过将此项设置为 `true`，您可以启用缓存共享功能，并允许系统在弹性伸缩期间选择正确的缓存节点。
- 引入版本: v3.5.1

### `es_state_sync_interval_second`

- 默认值: 10
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: FE 获取 Elasticsearch 索引并同步 StarRocks 外部表元数据的时间间隔。
- 引入版本: -

### `hive_meta_cache_refresh_interval_s`

- 默认值: 3600 * 2
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: Hive 外部表缓存元数据的更新时间间隔。
- 引入版本: -

### `hive_meta_store_timeout_s`

- 默认值: 10
- 类型: Long
- 单位: 秒
- 是否可变: No
- 描述: 连接 Hive metastore 的超时时长。
- 引入版本: -

### `jdbc_connection_idle_timeout_ms`

- 默认值: 600000
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 访问 JDBC Catalog 的连接超时最长时间。超时连接被视为空闲。
- 引入版本: -

### `jdbc_connection_timeout_ms`

- 默认值: 10000
- 类型: Long
- 单位: 毫秒
- 是否可变: No
- 描述: HikariCP 连接池获取连接的超时时间（毫秒）。如果在此时限内无法从池中获取连接，则操作将失败。
- 引入版本: v3.5.13

### `jdbc_query_timeout_ms`

- 默认值: 30000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: JDBC 语句查询执行的超时时间（毫秒）。此超时应用于通过 JDBC Catalog 执行的所有 SQL 查询（例如，分区元数据查询）。该值在传递给 JDBC 驱动程序时转换为秒。
- 引入版本: v3.5.13

### `jdbc_network_timeout_ms`

- 默认值: 30000
- 类型: Long
- 单位: 毫秒
- 是否可变: Yes
- 描述: JDBC 网络操作（套接字读取）的超时时间（毫秒）。此超时应用于数据库元数据调用（例如，getSchemas()、getTables()、getColumns()），以防止外部数据库无响应时无限期阻塞。
- 引入版本: v3.5.13

### `jdbc_connection_max_lifetime_ms`

- 默认值: 300000
- 类型: Long
- 单位: 毫秒
- 可变: 否
- 描述: JDBC 连接池中连接的最大生命周期。连接在此超时前会被回收，以防止连接失效。应短于外部数据库的连接超时。允许的最小值为 30000 (30 秒)。
- 引入版本: -

### `jdbc_connection_keepalive_time_ms`

- 默认值: 30000
- 类型: Long
- 单位: 毫秒
- 可变: 否
- 描述: 空闲 JDBC 连接的保活间隔。空闲连接会在此间隔进行测试，以主动检测失效连接。设置为 0 可禁用保活探测。启用时，必须 >= 30000 且小于 `jdbc_connection_max_lifetime_ms`。无效的启用值将被静默禁用（重置为 0）。
- 引入版本: -

### `jdbc_connection_leak_detection_threshold_ms`

- 默认值: 0
- 类型: Long
- 单位: 毫秒
- 可变: 否
- 描述: JDBC 连接泄漏检测的阈值。如果连接保持时间超过此值，将记录警告。设置为 0 可禁用。这是一个调试辅助工具，用于识别持有连接时间过长的代码路径。
- 引入版本: -

### `jdbc_connection_pool_size`

- 默认值: 8
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 访问 JDBC Catalog 的 JDBC 连接池的最大容量。
- 引入版本: -

### `jdbc_meta_default_cache_enable`

- 默认值: false
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: JDBC Catalog 元数据缓存是否启用的默认值。设置为 True 时，新创建的 JDBC Catalog 将默认启用元数据缓存。
- 引入版本: -

### `jdbc_meta_default_cache_expire_sec`

- 默认值: 600
- 类型: Long
- 单位: 秒
- 是否可变: Yes
- 描述: JDBC Catalog 元数据缓存的默认过期时间。当 `jdbc_meta_default_cache_enable` 设置为 true 时，新创建的 JDBC Catalog 将默认设置元数据缓存的过期时间。
- 引入版本: -

### `jdbc_minimum_idle_connections`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: 访问 JDBC Catalog 的 JDBC 连接池中的最小空闲连接数。
- 引入版本: -

### `jwt_jwks_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: JSON Web Key Set (JWKS) 服务的 URL 或 `fe/conf` 目录下公共密钥本地文件的路径。
- 引入版本: v3.5.0

### `jwt_principal_field`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中指示 subject (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本: v3.5.0

### `jwt_required_audience`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 audience (`aud`) 的字符串列表。只有当列表中一个值与 JWT audience 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

### `jwt_required_issuer`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 issuer (`iss`) 的字符串列表。只有当列表中一个值与 JWT issuer 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

### locale

- 默认值: `zh_CN.UTF-8`
- 类型: String
- 单位: -
- 是否可变: No
- 描述: FE 使用的字符集。
- 引入版本: -

### `max_agent_task_threads_num`

- 默认值: 4096
- 类型: Int
- 单位: -
- 是否可变: No
- 描述: agent 任务线程池中允许的最大线程数。
- 引入版本: -

### `max_download_task_per_be`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在每个 RESTORE 操作中，StarRocks 分配给 BE 节点的最大下载任务数。当此项设置为小于或等于 0 时，任务数量不受限制。
- 引入版本: v3.1.0

### `max_mv_check_base_table_change_retry_times`

- 默认值: 10
- 类型: -
- 单位: -
- 是否可变: Yes
- 描述: 刷新物化视图时检测基表变更的最大重试次数。
- 引入版本: v3.3.0

### `max_mv_refresh_failure_retry_times`

- 默认值: 1
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图刷新失败时的最大重试次数。
- 引入版本: v3.3.0

### `max_mv_refresh_try_lock_failure_retry_times`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图刷新失败时尝试锁定的最大重试次数。
- 引入版本: v3.3.0

### `max_small_file_number`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 可存储在 FE 目录中的小型文件最大数量。
- 引入版本: -

### `max_small_file_size_bytes`

- 默认值: 1024 * 1024
- 类型: Int
- 单位: 字节
- 是否可变: Yes
- 描述: 小型文件的最大大小。
- 引入版本: -

### `max_upload_task_per_be`

- 默认值: 0
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在每个 BACKUP 操作中，StarRocks 分配给 BE 节点的最大上传任务数。当此项设置为小于或等于 0 时，任务数量不受限制。
- 引入版本: v3.1.0

### `mv_create_partition_batch_interval_ms`

- 默认值: 1000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 在物化视图刷新期间，如果需要批量创建多个分区，系统会将其分为每批 64 个分区。为降低频繁创建分区导致失败的风险，每个批次之间设置了默认间隔（以毫秒为单位）以控制创建频率。
- 引入版本: v3.3

### `mv_plan_cache_max_size`

- 默认值: 1000
- 类型: Long
- 单位:
- 是否可变: Yes
- 描述: 物化视图计划缓存的最大大小（用于物化视图重写）。如果有很多物化视图用于透明查询重写，您可以增加此值。
- 引入版本: v3.2

### `mv_plan_cache_thread_pool_size`

- 默认值: 3
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 物化视图计划缓存的默认线程池大小（用于物化视图重写）。
- 引入版本: v3.2

### `mv_refresh_default_planner_optimize_timeout`

- 默认值: 30000
- 类型: -
- 单位: -
- 是否可变: Yes
- 描述: 刷新物化视图时优化器规划阶段的默认超时。
- 引入版本: v3.3.0

### `mv_refresh_fail_on_filter_data`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 如果刷新过程中存在过滤数据，物化视图刷新失败，默认为 true，否则忽略过滤数据并返回成功。
- 引入版本: -

### `mv_refresh_try_lock_timeout_ms`

- 默认值: 30000
- 类型: Int
- 单位: 毫秒
- 是否可变: Yes
- 描述: 物化视图刷新尝试对其基表/物化视图进行数据库锁定的默认尝试锁定超时。
- 引入版本: v3.3.0

### `oauth2_auth_server_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 授权 URL。用户浏览器将被重定向到此 URL，以开始 OAuth 2.0 授权过程。
- 引入版本: v3.5.0

### `oauth2_client_id`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: StarRocks 客户端的公共标识符。
- 引入版本: v3.5.0

### `oauth2_client_secret`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于授权 StarRocks 客户端与授权服务器通信的密钥。
- 引入版本: v3.5.0

### `oauth2_jwks_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: JSON Web Key Set (JWKS) 服务的 URL 或 `conf` 目录下本地文件的路径。
- 引入版本: v3.5.0

### `oauth2_principal_field`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中指示 subject (`sub`) 的字段的字符串。默认值为 `sub`。此字段的值必须与登录 StarRocks 的用户名相同。
- 引入版本: v3.5.0

### `oauth2_redirect_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: OAuth 2.0 认证成功后，用户浏览器将被重定向到的 URL。授权码将发送到此 URL。在大多数情况下，需要将其配置为 `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`。
- 引入版本: v3.5.0

### `oauth2_required_audience`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 audience (`aud`) 的字符串列表。只有当列表中一个值与 JWT audience 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

### `oauth2_required_issuer`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 用于标识 JWT 中 issuer (`iss`) 的字符串列表。只有当列表中一个值与 JWT issuer 匹配时，JWT 才被视为有效。
- 引入版本: v3.5.0

### `oauth2_token_server_url`

- 默认值: 空字符串
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 授权服务器上 StarRocks 获取访问令牌的 endpoint 的 URL。
- 引入版本: v3.5.0

### `plugin_dir`

- 默认值: `System.getenv("STARROCKS_HOME")` + "/plugins"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储插件安装包的目录。
- 引入版本: -

### `plugin_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否允许在 FE 上安装插件。插件只能在 Leader FE 上安装或卸载。
- 引入版本: -

### `proc_profile_jstack_depth`

- 默认值: 128
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 系统收集 CPU 和内存 profile 时的最大 Java 堆栈深度。此值控制每个采样堆栈捕获的 Java 堆栈帧数：较大的值会增加跟踪详细信息和输出大小，并可能增加 profiling 开销，而较小的值会减少详细信息。此设置在 CPU 和内存 profiling 启动 profiler 时使用，因此请根据诊断需求和性能影响进行调整。
- 引入版本: -

### `proc_profile_mem_enable`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 是否启用进程内存分配 profile 收集。当此项设置为 `true` 时，系统会在 `sys_log_dir/proc_profile` 下生成名为 `mem-profile-<timestamp>.html` 的 HTML profile，在采样期间休眠 `proc_profile_collect_time_s` 秒，并使用 `proc_profile_jstack_depth` 作为 Java 堆栈深度。生成的文件会根据 `proc_profile_file_retained_days` 和 `proc_profile_file_retained_size_bytes` 进行压缩和清除。原生提取路径使用 `STARROCKS_HOME_DIR` 以避免 `/tmp` noexec 问题。此项旨在用于故障排除内存分配热点。启用它会增加 CPU、I/O 和磁盘使用率，并可能生成大文件。
- 引入版本: v3.2.12

### `query_detail_explain_level`

- 默认值: COSTS
- 类型: String
- 单位: -
- 是否可变: true
- 描述: EXPLAIN 语句返回的查询计划的详细级别。有效值：COSTS, NORMAL, VERBOSE。
- 引入版本: v3.2.12, v3.3.5

### `replication_interval_ms`

- 默认值: 100
- 类型: Int
- 单位: 毫秒
- 是否可变: No
- 描述: 调度复制任务的最小时间间隔。
- 引入版本: v3.3.5

### `replication_max_parallel_data_size_mb`

- 默认值: 1048576
- 类型: Int
- 单位: MB
- 是否可变: Yes
- 描述: 允许并发同步的最大数据大小。
- 引入版本: v3.3.5

### `replication_max_parallel_replica_count`

- 默认值: 10240
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许并发同步的 tablet 副本的最大数量。
- 引入版本: v3.3.5

### `replication_max_parallel_table_count`

- 默认值: 100
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 允许的最大并发数据同步任务数。StarRocks 为每张表创建一个同步任务。
- 引入版本: v3.3.5

### `replication_transaction_timeout_sec`

- 默认值: 86400
- 类型: Int
- 单位: 秒
- 是否可变: Yes
- 描述: 同步任务的超时时长。
- 引入版本: v3.3.5

### `skip_whole_phase_lock_mv_limit`

- 默认值: 5
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 控制 StarRocks 何时对具有相关物化视图的表应用“无锁”优化。当此项设置为小于 0 时，系统始终应用无锁优化，并且不为查询复制相关物化视图（减少 FE 内存使用和元数据复制/锁争用，但可能增加元数据并发问题的风险）。当设置为 0 时，禁用无锁优化（系统始终使用安全的复制和锁定路径）。当设置为大于 0 时，仅当相关物化视图的数量小于或等于配置阈值时才应用无锁优化。此外，当值大于或等于 0 时，规划器会将查询 OLAP 表记录到优化器上下文中以启用与物化视图相关的重写路径；当值小于 0 时，此步骤将被跳过。
- 引入版本: v3.2.1

### `small_file_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/small_files"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 小型文件的根目录。
- 引入版本: -

### `task_runs_max_history_number`

- 默认值: 10000
- 类型: Int
- 单位: -
- 是否可变: Yes
- 描述: 在内存中保留的任务运行记录的最大数量，并用作查询存档任务运行历史记录时的默认 LIMIT。当 `enable_task_history_archive` 为 false 时，此值限制内存中的历史记录：强制 GC 会修剪较旧的条目，因此只保留最新的 `task_runs_max_history_number`。当查询存档历史记录时（且未提供显式 LIMIT），如果此值大于 0，`TaskRunHistoryTable.lookup` 将使用 `"ORDER BY create_time DESC LIMIT <value>"`。注意：将此值设置为 0 会禁用查询侧的 LIMIT（无上限），但会导致内存中的历史记录被截断为零（除非启用了存档）。
- 引入版本: v3.2.0

### `tmp_dir`

- 默认值: `StarRocksFE.STARROCKS_HOME_DIR` + "/temp_dir"
- 类型: String
- 单位: -
- 是否可变: No
- 描述: 存储临时文件的目录，例如备份和恢复过程中生成的文件。这些过程完成后，生成的临时文件将被删除。
- 引入版本: -

### `transform_type_prefer_string_for_varchar`

- 默认值: true
- 类型: Boolean
- 单位: -
- 是否可变: Yes
- 描述: 在物化视图创建和 CTAS 操作中，是否更倾向于为固定长度的 varchar 列使用 string 类型。
- 引入版本: v4.0.0
