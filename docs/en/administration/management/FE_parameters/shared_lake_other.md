---
displayed_sidebar: docs
sidebar_label: "Shared-data, Data Lake, and Others"
---

# FE Configuration - Shared-data, Data Lake, and Others

import FEConfigMethod from '../../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../../_assets/commonMarkdown/StaticFE_config_note.mdx'

<FEConfigMethod />

## View FE configuration items

After your FE is started, you can run the ADMIN SHOW FRONTEND CONFIG command on your MySQL client to check the parameter configurations. If you want to query the configuration of a specific parameter, run the following command:

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

For detailed description of the returned fields, see [`ADMIN SHOW CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md).

:::note
You must have administrator privileges to run cluster administration-related commands.
:::

## Configure FE parameters

### Configure FE dynamic parameters

You can configure or modify the settings of FE dynamic parameters using [`ADMIN SET FRONTEND CONFIG`](../../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md).

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### Configure FE static parameters

<StaticFEConfigNote />

---

This topic introduces the following types of FE configurations:
- [Shared-data](#shared-data)
- [Data Lake](#data-lake)
- [Other](#other)

## Shared-data

### `aws_s3_access_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Access Key ID used to access your S3 bucket.
- Introduced in: v3.0

### `aws_s3_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint used to access your S3 bucket, for example, `https://s3.us-west-2.amazonaws.com`.
- Introduced in: v3.0

### `aws_s3_external_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The external ID of the AWS account that is used for cross-account access to your S3 bucket.
- Introduced in: v3.0

### `aws_s3_iam_role_arn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The ARN of the IAM role that has privileges on your S3 bucket in which your data files are stored.
- Introduced in: v3.0

### `aws_s3_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The S3 path used to store data. It consists of the name of your S3 bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- Introduced in: v3.0

### `aws_s3_region`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The region in which your S3 bucket resides, for example, `us-west-2`.
- Introduced in: v3.0

### `aws_s3_secret_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Secret Access Key used to access your S3 bucket.
- Introduced in: v3.0

### `aws_s3_use_aws_sdk_default_behavior`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use the default authentication credential of AWS SDK. Valid values: true and false (Default).
- Introduced in: v3.0

### `aws_s3_use_instance_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use Instance Profile and Assumed Role as credential methods for accessing S3. Valid values: true and false (Default).
  - If you use IAM user-based credential (Access Key and Secret Key) to access S3, you must specify this item as `false`, and specify `aws_s3_access_key` and `aws_s3_secret_key`.
  - If you use Instance Profile to access S3, you must specify this item as `true`.
  - If you use Assumed Role to access S3, you must specify this item as `true`, and specify `aws_s3_iam_role_arn`.
  - And if you use an external AWS account, you must also specify `aws_s3_external_id`.
- Introduced in: v3.0

### `azure_adls2_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint of your Azure Data Lake Storage Gen2 Account, for example, `https://test.dfs.core.windows.net`.
- Introduced in: v3.4.1

### `azure_adls2_oauth2_client_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Client ID of the Managed Identity used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

### `azure_adls2_oauth2_tenant_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Tenant ID of the Managed Identity used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

### `azure_adls2_oauth2_use_managed_identity`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use Managed Identity to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.4

### `azure_adls2_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Azure Data Lake Storage Gen2 path used to store data. It consists of the file system name and the directory name, for example, `testfilesystem/starrocks`.
- Introduced in: v3.4.1

### `azure_adls2_sas_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The shared access signatures (SAS) used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.1

### `azure_adls2_shared_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Shared Key used to authorize requests for your Azure Data Lake Storage Gen2.
- Introduced in: v3.4.1

### `azure_blob_endpoint`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The endpoint of your Azure Blob Storage Account, for example, `https://test.blob.core.windows.net`.
- Introduced in: v3.1

### `azure_blob_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Azure Blob Storage path used to store data. It consists of the name of the container within your storage account and the sub-path (if any) under the container, for example, `testcontainer/subpath`.
- Introduced in: v3.1

### `azure_blob_sas_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The shared access signatures (SAS) used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

### `azure_blob_shared_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Shared Key used to authorize requests for your Azure Blob Storage.
- Introduced in: v3.1

### `azure_use_native_sdk`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to use the native SDK to access Azure Blob Storage, thus allowing authentication with Managed Identities and Service Principals. If this item is set to `false`, only authentication with Shared Key and SAS Token is allowed.
- Introduced in: v3.4.4

### `cloud_native_hdfs_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL of the HDFS storage, for example, `hdfs://127.0.0.1:9000/user/xxx/starrocks/`.
- Introduced in: -

### `cloud_native_meta_port`

- Default: 6090
- Type: Int
- Unit: -
- Is mutable: No
- Description: FE cloud-native metadata server RPC listen port.
- Introduced in: -

### `cloud_native_storage_type`

- Default: S3
- Type: String
- Unit: -
- Is mutable: No
- Description: The type of object storage you use. In shared-data mode, StarRocks supports storing data in HDFS, Azure Blob (supported from v3.1.1 onwards), Azure Data Lake Storage Gen2 (supported from v3.4.1 onwards), Google Storage (with native SDK, supported from v3.5.1 onwards), and object storage systems that are compatible with the S3 protocol (such as AWS S3, and MinIO). Valid value: `S3` (Default), `HDFS`, `AZBLOB`, `ADLS2`, and `GS`. If you specify this parameter as `S3`, you must add the parameters prefixed by `aws_s3`. If you specify this parameter as `AZBLOB`, you must add the parameters prefixed by `azure_blob`. If you specify this parameter as `ADLS2`, you must add the parameters prefixed by `azure_adls2`. If you specify this parameter as `GS`, you must add the parameters prefixed by `gcp_gcs`. If you specify this parameter as `HDFS`, you only need to specify `cloud_native_hdfs_url`.
- Introduced in: -

### `enable_load_volume_from_conf`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to allow StarRocks to create the built-in storage volume by using the object storage-related properties specified in the FE configuration file. The default value is changed from `true` to `false` from v3.4.1 onwards.
- Introduced in: v3.1.0

### `gcp_gcs_impersonation_service_account`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Service Account that you want to impersonate if you use the impersonation-based authentication to access Google Storage.
- Introduced in: v3.5.1

### `gcp_gcs_path`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Google Cloud path used to store data. It consists of the name of your Google Cloud bucket and the sub-path (if any) under it, for example, `testbucket/subpath`.
- Introduced in: v3.5.1

### `gcp_gcs_service_account_email`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The email address in the JSON file generated at the creation of the Service Account, for example, `user@hello.iam.gserviceaccount.com`.
- Introduced in: v3.5.1

### `gcp_gcs_service_account_private_key`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Private Key in the JSON file generated at the creation of the Service Account, for example, `-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n`.
- Introduced in: v3.5.1

### `gcp_gcs_service_account_private_key_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The Private Key ID in the JSON file generated at the creation of the Service Account.
- Introduced in: v3.5.1

### `gcp_gcs_use_compute_engine_service_account`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Whether to use the Service Account that is bound to your Compute Engine.
- Introduced in: v3.5.1

### `hdfs_file_system_expire_seconds`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: Time-to-live in seconds for an unused cached HDFS/ObjectStore FileSystem managed by HdfsFsManager. The FileSystemExpirationChecker (runs every 60s) calls each HdfsFs.isExpired(...) using this value; when expired the manager closes the underlying FileSystem and removes it from the cache. Accessor methods (for example `HdfsFs.getDFSFileSystem`, `getUserName`, `getConfiguration`) update the last-access timestamp, so expiry is based on inactivity. Lower values reduce idle resource holding but increase reopen overhead; higher values keep handles longer and may consume more resources.
- Introduced in: v3.2.0

### `lake_autovacuum_grace_period_minutes`

- Default: 30
- Type: Long
- Unit: Minutes
- Is mutable: Yes
- Description: The time range for retaining historical data versions in a shared-data cluster. Historical data versions within this time range are not automatically cleaned via AutoVacuum after Compactions. You need to set this value greater than the maximum query time to avoid that the data accessed by running queries get deleted before the queries finish. The default value has been changed from `5` to `30` since v3.3.0, v3.2.5, and v3.1.10.
- Introduced in: v3.1.0

### `lake_autovacuum_parallel_partitions`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of partitions that can undergo AutoVacuum simultaneously in a shared-data cluster. AutoVacuum is the Garbage Collection after Compactions.
- Introduced in: v3.1.0

### `lake_autovacuum_partition_naptime_seconds`

- Default: 180
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The minimum interval between AutoVacuum operations on the same partition in a shared-data cluster.
- Introduced in: v3.1.0

### `lake_autovacuum_stale_partition_threshold`

- Default: 12
- Type: Long
- Unit: Hours
- Is mutable: Yes
- Description: If a partition has no updates (loading, DELETE, or Compactions) within this time range, the system will not perform AutoVacuum on this partition.
- Introduced in: v3.1.0

### `lake_compaction_allow_partial_success`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: If this item is set to `true`, the system will consider the Compaction operation in a shared-data cluster as successful when one of the sub-tasks succeeds.
- Introduced in: v3.5.2

### `lake_compaction_disable_ids`

- Default: ""
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The table or partition list of which compaction is disabled in shared-data mode. The format is `tableId1;partitionId2`, seperated by semicolon, for example, `12345;98765`.
- Introduced in: v3.4.4

### `lake_compaction_history_size`

- Default: 20
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The number of recent successful Compaction task records to keep in the memory of the Leader FE node in a shared-data cluster. You can view recent successful Compaction task records using the `SHOW PROC '/compactions'` command. Note that the Compaction history is stored in the FE process memory, and it will be lost if the FE process is restarted.
- Introduced in: v3.1.0

### `lake_compaction_max_parallel_default`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Default max parallel compaction subtasks per tablet when `lake_compaction_max_parallel` is not specified in table properties. `0` means disable parallel compaction. This config is used as the default value for the table property `lake_compaction_max_parallel`.

### `lake_compaction_max_tasks`

- Default: -1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent Compaction tasks allowed in a shared-data cluster. Setting this item to `-1` indicates to calculate the concurrent task number in an adaptive manner. Setting this value to `0` will disable compaction.
- Introduced in: v3.1.0

### `lake_compaction_score_selector_min_score`

- Default: 10.0
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Compaction operations in a shared-data cluster. When the Compaction Score of a partition is greater than or equal to this value, the system performs Compaction on that partition.
- Introduced in: v3.1.0

### `lake_compaction_score_upper_bound`

- Default: 2000
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The upper limit of the Compaction Score for a partition in a shared-data cluster. `0` indicates no upper limit. This item only takes effect when `lake_enable_ingest_slowdown` is set to `true`. When the Compaction Score of a partition reaches or exceeds this upper limit, incoming loading tasks will be rejected. From v3.3.6 onwards, the default value is changed from `0` to `2000`.
- Introduced in: v3.2.0

### `lake_compaction_interval_ms_on_success`

- Default: 10000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The interval before triggering the next Compaction for a partition in a shared-data cluster after a successful Compaction on that partition. The alias is `lake_min_compaction_interval_ms_on_success`.
- Introduced in: v3.2.0

### `lake_enable_balance_tablets_between_workers`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to balance the number of tablets among Compute Nodes during the tablet migration of cloud-native tables in a shared-data cluster. `true` indicates to balance the tablets among Compute Nodes, and `false` indicates to disabling this feature.
- Introduced in: v3.3.4

### `lake_enable_ingest_slowdown`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Data Ingestion Slowdown in a shared-data cluster. When Data Ingestion Slowdown is enabled, if the Compaction Score of a partition exceeds `lake_ingest_slowdown_threshold`, loading tasks on that partition will be throttled down. This configuration only takes effect when `run_mode` is set to `shared_data`. From v3.3.6 onwards, the default value is chenged from `false` to `true`.
- Introduced in: v3.2.0

### `lake_ingest_slowdown_threshold`

- Default: 100
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The Compaction Score threshold that triggers Data Ingestion Slowdown in a shared-data cluster. This configuration only takes effect when `lake_enable_ingest_slowdown` is set to `true`.
- Introduced in: v3.2.0

### `lake_publish_version_max_threads`

- Default: 512
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of threads for Version Publish tasks in a shared-data cluster.
- Introduced in: v3.2.0

### `meta_sync_force_delete_shard_meta`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow deleting the metadata of the shared-data cluster directly, bypassing cleaning the remote storage files. It is recommended to set this item to `true` only when there is an excessive number of shards to be cleaned, which leads to extreme memory pressure on the FE JVM. Note that the data files belonging to the shards or tablets cannot be automatically cleaned after this feature is enabled.
- Introduced in: v3.2.10, v3.3.3

### `run_mode`

- Default: `shared_nothing`
- Type: String
- Unit: -
- Is mutable: No
- Description: The running mode of the StarRocks cluster. Valid values: `shared_data` and `shared_nothing` (Default).
  - `shared_data` indicates running StarRocks in shared-data mode.
  - `shared_nothing` indicates running StarRocks in shared-nothing mode.

  > **CAUTION**
  >
  > - You cannot adopt the `shared_data` and `shared_nothing` modes simultaneously for a StarRocks cluster. Mixed deployment is not supported.
  > - DO NOT change `run_mode` after the cluster is deployed. Otherwise, the cluster fails to restart. The transformation from a shared-nothing cluster to a shared-data cluster or vice versa is not supported.

- Introduced in: -

### `shard_group_clean_threshold_sec`

- Default: 3600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The time before FE cleans the unused tablet and shard groups in a shared-data cluster. Tablets and shard groups created within this threshold will not be cleaned.
- Introduced in: -

### `star_mgr_meta_sync_interval_sec`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The interval at which FE runs the periodical metadata synchronization with StarMgr in a shared-data cluster.
- Introduced in: -

### `starmgr_grpc_server_max_worker_threads`

- Default: 1024
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of worker threads that are used by the grpc server in the FE starmgr module.
- Introduced in: v4.0.0, v3.5.8

### `starmgr_grpc_timeout_seconds`

- Default: 5
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description:
- Introduced in: -

## Data Lake

### `files_enable_insert_push_down_schema`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, the analyzer will attempt to push the target table schema into the `files()` table function for INSERT ... FROM files() operations. This only applies when the source is a FileTableFunctionRelation, the target is a native table, and the SELECT list contains corresponding slot-ref columns (or *). The analyzer will match select columns to target columns (counts must match), lock the target table briefly, and replace file-column types with deep-copied target column types for non-complex types (complex types such as Parquet JSON `->` `array<varchar>` are skipped). Column names from the original files table are preserved. This reduces type-mismatch and looseness from file-based type inference during ingestion.
- Introduced in: v3.4.0, v3.5.0

### `hdfs_read_buffer_size_kb`

- Default: 8192
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: Size of the HDFS read buffer in kilobytes. StarRocks converts this value to bytes (`<< 10`) and uses it to initialize HDFS read buffers in `HdfsFsManager` and to populate the thrift field `hdfs_read_buffer_size_kb` sent to BE tasks (e.g., `TBrokerScanRangeParams`, `TDownloadReq`) when broker access is not used. Increasing `hdfs_read_buffer_size_kb` can improve sequential read throughput and reduce syscall overhead at the cost of higher per-stream memory usage; decreasing it reduces memory footprint but may lower IO efficiency. Consider workload (many small streams vs. few large sequential reads) when tuning.
- Introduced in: v3.2.0

### `hdfs_write_buffer_size_kb`

- Default: 1024
- Type: Int
- Unit: Kilobytes
- Is mutable: Yes
- Description: Sets the HDFS write buffer size (in KB) used for direct writes to HDFS or object stores when not using a broker. The FE converts this value to bytes (`<< 10`) and initializes the local write buffer in HdfsFsManager, and it is propagated in Thrift requests (e.g., TUploadReq, TExportSink, sink options) so backends/agents use the same buffer size. Increasing this value can improve throughput for large sequential writes at the cost of more memory per writer; decreasing it reduces per-stream memory usage and may lower latency for small writes. Tune alongside `hdfs_read_buffer_size_kb` and consider available memory and concurrent writers.
- Introduced in: v3.2.0

### `lake_batch_publish_max_version_num`

- Default: 10
- Type: Int
- Unit: Count
- Is mutable: Yes
- Description: Sets the upper bound on how many consecutive transaction versions may be grouped together when building a publish batch for lake (cloud‑native) tables. The value is passed to the transaction graph batching routine (see getReadyToPublishTxnListBatch) and works together with `lake_batch_publish_min_version_num` to determine the candidate range size for a TransactionStateBatch. Larger values can increase publish throughput by batching more commits, but increase the scope of an atomic publish (longer visibility latency and larger rollback surface) and may be limited at runtime when versions are not consecutive. Tune according to workload and visibility/latency requirements.
- Introduced in: v3.2.0

### `lake_batch_publish_min_version_num`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Sets the minimum number of consecutive transaction versions required to form a publish batch for lake tables. DatabaseTransactionMgr.getReadyToPublishTxnListBatch passes this value to transactionGraph.getTxnsWithTxnDependencyBatch together with `lake_batch_publish_max_version_num` to select dependent transactions. A value of `1` allows single-transaction publishes (no batching). Values `>1` require at least that many consecutively-versioned, single-table, non-replication transactions to be available; batching is aborted if versions are non-consecutive, a replication transaction appears, or a schema change consumes a version. Increasing this value can improve publish throughput by grouping commits but may delay publishing while waiting for enough consecutive transactions.
- Introduced in: v3.2.0

### `lake_enable_batch_publish_version`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, PublishVersionDaemon batches ready transactions for the same Lake (shared-data) table/partition and publishes their versions together instead of issuing per-transaction publishes. In RunMode shared-data, the daemon calls getReadyPublishTransactionsBatch() and uses publishVersionForLakeTableBatch(...) to perform grouped publish operations (reducing RPCs and improving throughput). When disabled, the daemon falls back to per-transaction publishing via publishVersionForLakeTable(...). The implementation coordinates in-flight work using internal sets to avoid duplicate publishes when the switch is toggled and is affected by the thread pool sizing via `lake_publish_version_max_threads`.
- Introduced in: v3.2.0

### `lake_enable_tablet_creation_optimization`

- Default: false
- Type: boolean
- Unit: -
- Is mutable: Yes
- Description: When enabled, StarRocks optimizes tablet creation for cloud-native tables and materialized views in shared-data mode by creating a single shared tablet metadata for all tablets under a physical partition instead of distinct metadata per tablet. This reduces the number of tablet creation tasks and metadata/files produced during table creation, rollup, and schema-change jobs. The optimization is applied only for cloud-native tables/materialized views and is combined with `file_bundling` (the latter reuses the same optimization logic). Note: schema-change and rollup jobs explicitly disable the optimization for tables using `file_bundling` to avoid overwriting files with identical names. Enable cautiously — it changes the granularity of created tablet metadata and can affect how replica creation and file naming behave.
- Introduced in: v3.3.1, v3.4.0, v3.5.0

### `lake_use_combined_txn_log`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: When this item is set to `true`, the system allows Lake tables to use the combined transaction log path for relevant transactions. Available for shared-data clusters only.
- Introduced in: v3.3.7, v3.4.0, v3.5.0

### `lake_repair_metadata_fetch_max_version_batch_size`

- Default: 160
- Type: Long
- Unit: -
- Is mutable: Yes
- Description: The maximum batch size for version scanning when fetching tablet metadata during lake tablet repair. The batch size starts at 5 and grows exponentially (doubling each iteration) up to this maximum. A larger value allows more versions to be fetched in a single batch, which can improve repair efficiency by leveraging file existence caching across versions. If set to a value less than 5, it will be clamped to 5 at runtime.
- Introduced in: v3.5.16, v4.0.9

### `lake_enable_drop_tablet_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: In shared-data mode, clears the cache on BE/CN before the underlying data is actually deleted.
- Introduced in: v4.0


### `enable_iceberg_commit_queue`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable commit queue for Iceberg tables to avoid concurrent commit conflicts. Iceberg uses optimistic concurrency control (OCC) for metadata commits. When multiple threads concurrently commit to the same table, conflicts can occur with errors like: "Cannot commit: Base metadata location is not same as the current table metadata location". When enabled, each Iceberg table has its own single-threaded executor for commit operations, ensuring that commits to the same table are serialized and preventing OCC conflicts. Different tables can commit concurrently, maintaining overall throughput. This is a system-level optimization to improve reliability and should be enabled by default. If disabled, concurrent commits may fail due to optimistic locking conflicts.
- Introduced in: v4.1.0

### `iceberg_commit_queue_timeout_seconds`

- Default: 300
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout in seconds for waiting for an Iceberg commit operation to complete. When using the commit queue (`enable_iceberg_commit_queue=true`), each commit operation must complete within this timeout. If a commit takes longer than this timeout, it will be cancelled and an error will be raised. Factors that affect commit time include: number of data files being committed, metadata size of the table, performance of the underlying storage (e.g., S3, HDFS).
- Introduced in: v4.1.0

### `iceberg_commit_queue_max_size`

- Default: 1000
- Type: Int
- Unit: Count
- Is mutable: No
- Description: The maximum number of pending commit operations per Iceberg table. When using the commit queue (`enable_iceberg_commit_queue=true`), this limits the number of commit operations that can be queued for a single table. When the limit is reached, additional commit operations will execute in the caller thread (blocking until capacity available). This configuration is read at FE startup and applies to newly created table executors. Requires FE restart to take effect. Increase this value if you expect many concurrent commits to the same table. If this value is too low, commits may block in the caller thread during high concurrency.
- Introduced in: v4.1.0

##### lake_balance_tablets_threshold

- Default: 0.15
- Type: Double
- Unit: -
- Is mutable: Yes
- Description: The threshold the system used to judge the tablet balance among workers in a shared-data cluster, The imbalance factor is calculated as `f = (MAX(tablets) - MIN(tablets)) / AVERAGE(tablets)`. If the factor is greater than `lake_balance_tablets_threshold`, a tablet balance will be triggered. This item takes effect only when `lake_enable_balance_tablets_between_workers` is set to `true`.
- Introduced in: v3.3.4

## Other

### `agent_task_resend_wait_time_ms`

- Default: 5000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The duration the FE must wait before it can resend an agent task. An agent task can be resent only when the gap between the task creation time and the current time exceeds the value of this parameter. This parameter is used to prevent repetitive sending of agent tasks.
- Introduced in: -

### `allow_system_reserved_names`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow users to create columns whose names are initiated with `__op` and `__row`. To enable this feature, set this parameter to `TRUE`. Please note that these name formats are reserved for special purposes in StarRocks and creating such columns may result in undefined behavior. Therefore this feature is disabled by default.
- Introduced in: v3.2.0

### `auth_token`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The token that is used for identity authentication within the StarRocks cluster to which the FE belongs. If this parameter is left unspecified, StarRocks generates a random token for the cluster at the time when the leader FE of the cluster is started for the first time.
- Introduced in: -

### `authentication_ldap_simple_bind_base_dn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The base DN, which is the point from which the LDAP server starts to search for users' authentication information.
- Introduced in: -

### `authentication_ldap_simple_bind_root_dn`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The administrator DN used to search for users' authentication information.
- Introduced in: -

### `authentication_ldap_simple_bind_root_pwd`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The password of the administrator used to search for users' authentication information.
- Introduced in: -

### `authentication_ldap_simple_server_host`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The host on which the LDAP server runs.
- Introduced in: -

### `authentication_ldap_simple_server_port`

- Default: 389
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The port of the LDAP server.
- Introduced in: -

### `authentication_ldap_simple_user_search_attr`

- Default: uid
- Type: String
- Unit: -
- Is mutable: Yes
- Description: The name of the attribute that identifies users in LDAP objects.
- Introduced in: -

### `backup_job_default_timeout_ms`

- Default: 86400 * 1000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout duration of a backup job. If this value is exceeded, the backup job fails.
- Introduced in: -

### `enable_collect_tablet_num_in_show_proc_backend_disk_path`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable the collection of tablet numbers for each disk in the `SHOW PROC /BACKENDS/{id}` command
- Introduced in: v4.0.1, v3.5.8

### `enable_colocate_restore`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable Backup and Restore for Colocate Tables. `true` indicates enabling Backup and Restore for Colocate Tables and `false` indicates disabling it.
- Introduced in: v3.2.10, v3.3.3

### `enable_external_catalog_information_schema_tables_access_full_metadata`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Controls whether `information_schema.tables` is allowed to access external metadata services when resolving tables in external catalogs (such as Hive, Iceberg, JDBC). When set to `false` (default), columns like `TABLE_COMMENT` may be empty for external tables but the query is fast and avoids remote calls. When set to `true`, the FE contacts the corresponding external metadata service and can populate fields like `TABLE_COMMENT` at the cost of additional latency and remote calls per table.
- Introduced in: -

### `enable_materialized_view_concurrent_prepare`

- Default: true
- Type: Boolean
- Unit:
- Is mutable: Yes
- Description: Whether to prepare materialized view concurrently to improve performance.
- Introduced in: v3.4.4

### `enable_metric_calculator`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: No
- Description: Specifies whether to enable the feature that is used to periodically collect metrics. Valid values: `TRUE` and `FALSE`. `TRUE` specifies to enable this feature, and `FALSE` specifies to disable this feature.
- Introduced in: -

### `enable_table_metrics_collect`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to export table-level metrics in FE. When disabled, FE will skip exporting table metrics (such as table scan/load counters and table size metrics), but still records the counters in memory.
- Introduced in: -

### `enable_mv_post_image_reload_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to perform reload flag check after FE loaded an image. If the check is performed for a base materialized view, it is not needed for other materialized views that related to it.
- Introduced in: v3.5.0

### `enable_mv_query_context_cache`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable query-level materialized view rewrite cache to improve query rewrite performance.
- Introduced in: v3.3

### `enable_mv_refresh_collect_profile`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable profile in refreshing materialized view by default for all materialized views.
- Introduced in: v3.3.0

### `enable_mv_refresh_extra_prefix_logging`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable prefixes with materialized view names in logs for better debug.
- Introduced in: v3.4.0

### `enable_mv_refresh_query_rewrite`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable rewrite query during materialized view refresh so that the query can use the rewritten mv directly rather than the base table to improve query performance.
- Introduced in: v3.3

### `enable_trace_historical_node`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to allow the system to trace the historical nodes. By setting this item to `true`, you can enable the Cache Sharing feature and allow the system to choose the right cache nodes during elastic scaling.
- Introduced in: v3.5.1

### `es_state_sync_interval_second`

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the FE obtains Elasticsearch indexes and synchronizes the metadata of StarRocks external tables.
- Introduced in: -

### `hive_meta_cache_refresh_interval_s`

- Default: 3600 * 2
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The time interval at which the cached metadata of Hive external tables is updated.
- Introduced in: -

### `hive_meta_store_timeout_s`

- Default: 10
- Type: Long
- Unit: Seconds
- Is mutable: No
- Description: The amount of time after which a connection to a Hive metastore times out.
- Introduced in: -

### `jdbc_connection_idle_timeout_ms`

- Default: 600000
- Type: Int
- Unit: Milliseconds
- Is mutable: No
- Description: The maximum amount of time after which a connection for accessing a JDBC catalog times out. Timed-out connections are considered idle.
- Introduced in: -

### `jdbc_connection_timeout_ms`

- Default: 10000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: The timeout in milliseconds for HikariCP connection pool to acquire a connection. If a connection cannot be acquired from the pool within this time, the operation will fail.
- Introduced in: v3.5.13

### `jdbc_query_timeout_ms`

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout in milliseconds for JDBC statement query execution. This timeout is applied to all SQL queries executed through JDBC catalogs (e.g., partition metadata queries). The value is converted to seconds when passed to the JDBC driver.
- Introduced in: v3.5.13

### `jdbc_network_timeout_ms`

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: Yes
- Description: The timeout in milliseconds for JDBC network operations (socket read). This timeout applies to database metadata calls (e.g., getSchemas(), getTables(), getColumns()) to prevent indefinite blocking when the external database is unresponsive.
- Introduced in: v3.5.13

### `jdbc_connection_max_lifetime_ms`

- Default: 300000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: Maximum lifetime of a connection in the JDBC connection pool. Connections are recycled before this timeout to prevent stale connections. Should be shorter than the external database's connection timeout. Minimum allowed value is 30000 (30 seconds).
- Introduced in: -

### `jdbc_connection_keepalive_time_ms`

- Default: 30000
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: Keepalive interval for idle JDBC connections. Idle connections are tested at this interval to detect stale connections proactively. Set to 0 to disable keepalive probing. When enabled, must be >= 30000 and less than `jdbc_connection_max_lifetime_ms`. Invalid enabled values are silently disabled (reset to 0).
- Introduced in: -

### `jdbc_connection_leak_detection_threshold_ms`

- Default: 0
- Type: Long
- Unit: Milliseconds
- Is mutable: No
- Description: Threshold for JDBC connection leak detection. If a connection is held longer than this, a warning is logged. Set to 0 to disable. This is a debugging aid for identifying code paths that hold connections too long.
- Introduced in: -

### `jdbc_connection_pool_size`

- Default: 8
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum capacity of the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

### `jdbc_meta_default_cache_enable`

- Default: false
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: The default value for whether the JDBC Catalog metadata cache is enabled. When set to True, newly created JDBC Catalogs will default to metadata caching enabled.
- Introduced in: -

### `jdbc_meta_default_cache_expire_sec`

- Default: 600
- Type: Long
- Unit: Seconds
- Is mutable: Yes
- Description: The default expiration time for the JDBC Catalog metadata cache. When `jdbc_meta_default_cache_enable` is set to true, newly created JDBC Catalogs will default to setting the expiration time of the metadata cache.
- Introduced in: -

### `jdbc_minimum_idle_connections`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum number of idle connections in the JDBC connection pool for accessing JDBC catalogs.
- Introduced in: -

### `jwt_jwks_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the public key local file under the `fe/conf` directory.
- Introduced in: v3.5.0

### `jwt_principal_field`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- Introduced in: v3.5.0

### `jwt_required_audience`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.
- Introduced in: v3.5.0

### `jwt_required_issuer`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- Introduced in: v3.5.0

### locale

- Default: `zh_CN.UTF-8`
- Type: String
- Unit: -
- Is mutable: No
- Description: The character set that is used by the FE.
- Introduced in: -

### `max_agent_task_threads_num`

- Default: 4096
- Type: Int
- Unit: -
- Is mutable: No
- Description: The maximum number of threads that are allowed in the agent task thread pool.
- Introduced in: -

### `max_download_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each RESTORE operation, the maximum number of download tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

### `max_mv_check_base_table_change_retry_times`

- Default: 10
- Type: -
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times for detecting base table change when refreshing materialized views.
- Introduced in: v3.3.0

### `max_mv_refresh_failure_retry_times`

- Default: 1
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times when materialized view fails to refresh.
- Introduced in: v3.3.0

### `max_mv_refresh_try_lock_failure_retry_times`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum retry times of try lock when materialized view fails to refresh.
- Introduced in: v3.3.0

### `max_small_file_number`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of small files that can be stored on an FE directory.
- Introduced in: -

### `max_small_file_size_bytes`

- Default: 1024 * 1024
- Type: Int
- Unit: Bytes
- Is mutable: Yes
- Description: The maximum size of a small file.
- Introduced in: -

### `max_upload_task_per_be`

- Default: 0
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: In each BACKUP operation, the maximum number of upload tasks StarRocks assigned to a BE node. When this item is set to less than or equal to 0, no limit is imposed on the task number.
- Introduced in: v3.1.0

### `mv_create_partition_batch_interval_ms`

- Default: 1000
- Type: Int
- Unit: ms
- Is mutable: Yes
- Description: During materialized view refresh, if multiple partitions need to be created in bulk, the system divides them into batches of 64 partitions each. To reduce the risk of failures caused by frequent partition creation, a default interval (in milliseconds) is set between each batch to control the creation frequency.
- Introduced in: v3.3

### `mv_plan_cache_max_size`

- Default: 1000
- Type: Long
- Unit:
- Is mutable: Yes
- Description: The maximum size of materialized view plan cache (which is used for materialized view rewrite). If there are many materialized views used for transparent query rewrite, you may increase this value.
- Introduced in: v3.2

### `mv_plan_cache_thread_pool_size`

- Default: 3
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The default thread pool size of materialized view plan cache (which is used for materialized view rewrite).
- Introduced in: v3.2

### `mv_refresh_default_planner_optimize_timeout`

- Default: 30000
- Type: -
- Unit: -
- Is mutable: Yes
- Description: The default timeout for the planning phase of the optimizer when refresh materialized views.
- Introduced in: v3.3.0

### `mv_refresh_fail_on_filter_data`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Mv refresh fails if there is filtered data in refreshing, true by default, otherwise return success by ignoring the filtered data.
- Introduced in: -

### `mv_refresh_try_lock_timeout_ms`

- Default: 30000
- Type: Int
- Unit: Milliseconds
- Is mutable: Yes
- Description: The default try lock timeout for materialized view refresh to try the DB lock of its base table/materialized view.
- Introduced in: v3.3.0

### `oauth2_auth_server_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The authorization URL. The URL to which the users’ browser will be redirected in order to begin the OAuth 2.0 authorization process.
- Introduced in: v3.5.0

### `oauth2_client_id`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The public identifier of the StarRocks client.
- Introduced in: v3.5.0

### `oauth2_client_secret`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The secret used to authorize StarRocks client with the authorization server.
- Introduced in: v3.5.0

### `oauth2_jwks_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to the JSON Web Key Set (JWKS) service or the path to the local file under the `conf` directory.
- Introduced in: v3.5.0

### `oauth2_principal_field`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The string used to identify the field that indicates the subject (`sub`) in the JWT. The default value is `sub`. The value of this field must be identical with the username for logging in to StarRocks.
- Introduced in: v3.5.0

### `oauth2_redirect_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL to which the users’ browser will be redirected after the OAuth 2.0 authentication succeeds. The authorization code will be sent to this URL. In most cases, it need to be configured as `http://<starrocks_fe_url>:<fe_http_port>/api/oauth2`.
- Introduced in: v3.5.0

### `oauth2_required_audience`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the audience (`aud`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT audience.
- Introduced in: v3.5.0

### `oauth2_required_issuer`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The list of strings used to identify the issuers (`iss`) in the JWT. The JWT is considered valid only if one of the values in the list match the JWT issuer.
- Introduced in: v3.5.0

### `oauth2_token_server_url`

- Default: Empty string
- Type: String
- Unit: -
- Is mutable: No
- Description: The URL of the endpoint on the authorization server from which StarRocks obtains the access token.
- Introduced in: v3.5.0

### `plugin_dir`

- Default: `System.getenv("STARROCKS_HOME")` + "/plugins"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores plugin installation packages.
- Introduced in: -

### `plugin_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether plugins can be installed on FEs. Plugins can be installed or uninstalled only on the Leader FE.
- Introduced in: -

### `proc_profile_jstack_depth`

- Default: 128
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum Java stack depth when the system collects CPU and memory profiles. This value controls how many Java stack frames are captured for each sampled stack: larger values increase trace detail and output size and may add profiling overhead, while smaller values reduce details. This setting is used when the profiler is started for both CPU and memory profiling, so adjust it to balance diagnostic needs and performance impact.
- Introduced in: -

### `proc_profile_mem_enable`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to enable collection of process memory allocation profiles. When this item is set to `true`, the system generates an HTML profile named `mem-profile-<timestamp>.html` under `sys_log_dir/proc_profile`, sleeps for `proc_profile_collect_time_s` seconds while sampling, and uses `proc_profile_jstack_depth` for Java stack depth. Generated files are compressed and purged according to `proc_profile_file_retained_days` and `proc_profile_file_retained_size_bytes`. The native extraction path uses `STARROCKS_HOME_DIR` to avoid `/tmp` noexec issues. This item is intended for troubleshooting memory-allocation hotspots. Enabling it increases CPU, I/O and disk usage and may produce large files.
- Introduced in: v3.2.12

### `query_detail_explain_level`

- Default: COSTS
- Type: String
- Unit: -
- Is mutable: true
- Description: The detail level of query plan returned by the EXPLAIN statement. Valid values: COSTS, NORMAL, VERBOSE.
- Introduced in: v3.2.12, v3.3.5

### `replication_interval_ms`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: No
- Description: The minimum time interval at which the replication tasks are scheduled.
- Introduced in: v3.3.5

### `replication_max_parallel_data_size_mb`

- Default: 1048576
- Type: Int
- Unit: MB
- Is mutable: Yes
- Description: The maximum size of data allowed for concurrent synchronization.
- Introduced in: v3.3.5

### `replication_max_parallel_replica_count`

- Default: 10240
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of tablet replicas allowed for concurrent synchronization.
- Introduced in: v3.3.5

### `replication_max_parallel_table_count`

- Default: 100
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: The maximum number of concurrent data synchronization tasks allowed. StarRocks creates one synchronization task for each table.
- Introduced in: v3.3.5

### `replication_transaction_timeout_sec`

- Default: 86400
- Type: Int
- Unit: Seconds
- Is mutable: Yes
- Description: The timeout duration for synchronization tasks.
- Introduced in: v3.3.5

### `skip_whole_phase_lock_mv_limit`

- Default: 5
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Controls when StarRocks applies the "non-lock" optimization for tables that have related materialized views. When this item is set to less than 0, the system always applies non-lock optimization and does not copy related materialized views for queries (FE memory usage and metadata copy/lock contention is reduced but risk of metadata concurrency issues can be increased). When it is set to 0, non-lock optimization is disable (the system always use the safe, copy-and-lock path). When it is set to greater than 0, non-lock optimization is applied only for tables whose number of related materialized views is less than or equal to the configured threshold. Additionally, when the value is greater than and equal to 0, the planner records query OLAP tables into the optimizer context to enable materialized view-related rewrite paths; when it is less than 0, this step is skipped.
- Introduced in: v3.2.1

### `small_file_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/small_files"
- Type: String
- Unit: -
- Is mutable: No
- Description: The root directory of small files.
- Introduced in: -

### `task_runs_max_history_number`

- Default: 10000
- Type: Int
- Unit: -
- Is mutable: Yes
- Description: Maximum number of task run records to retain in memory and to use as a default LIMIT when querying archived task-run history. When `enable_task_history_archive` is false, this value bounds in-memory history: Force GC trims older entries so only the newest `task_runs_max_history_number` remain. When archive history is queried (and no explicit LIMIT is provided), `TaskRunHistoryTable.lookup` uses `"ORDER BY create_time DESC LIMIT <value>"` if this value is greater than 0. Note: setting this to 0 disables the query-side LIMIT (no cap) but will cause in-memory history to be truncated to zero (unless archiving is enabled).
- Introduced in: v3.2.0

### `tmp_dir`

- Default: `StarRocksFE.STARROCKS_HOME_DIR` + "/temp_dir"
- Type: String
- Unit: -
- Is mutable: No
- Description: The directory that stores temporary files such as files generated during backup and restore procedures. After these procedures finish, the generated temporary files are deleted.
- Introduced in: -

### `transform_type_prefer_string_for_varchar`

- Default: true
- Type: Boolean
- Unit: -
- Is mutable: Yes
- Description: Whether to prefer string type for fixed length varchar columns in materialized view creation and CTAS operations.
- Introduced in: v4.0.0

