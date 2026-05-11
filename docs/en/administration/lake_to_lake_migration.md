---
displayed_sidebar: docs
---

# Cross-cluster Data Migration Tool for Shared-Data Clusters

The StarRocks Cross-cluster Data Migration Tool supports migrating data between two shared-data (cloud-native) StarRocks clusters. During migration, the target cluster's Compute Nodes (CNs) copy data files directly from the source cluster's object storage to the target cluster's object storage — no BE-to-BE network transfer is required.

:::note

- This guide only applies to migrating from a **shared-data** source cluster to a **shared-data** target cluster. To migrate from a shared-nothing cluster, see [Cross-cluster Data Migration Tool](./data_migration_tool.md).
- The target cluster must be v4.1 or later.
- The target cluster cannot be a shared-nothing cluster.

:::

## Preparations

### Source cluster preparations

During migration, the source cluster's auto-vacuum mechanism may delete historical data versions that the target CNs still need to read. You must extend the vacuum grace period before starting migration to prevent this.

1. Increase `lake_autovacuum_grace_period_minutes` to a very large value:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="10000000");
   ```

   :::warning
   This setting prevents the source cluster from reclaiming stale object storage files during migration, which will cause storage amplification. Keep the migration window as short as possible. After migration is complete, remember to reset this value to the default (`30`).
   :::

2. After migration is complete, reset the value:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

### Target cluster preparations

The following preparations must be performed on the target cluster before starting migration.

#### Open ports

If you have enabled a firewall, open the following FE ports:

| **Component** | **Port**   | **Default** |
| ------------- | ---------- | ----------- |
| FE            | query_port | 9030 |
| FE            | http_port  | 8030 |
| FE            | rpc_port   | 9020 |

:::note
Because data is transferred directly between object storage systems, you do **not** need to open CN or BE ports from the source cluster.
:::

#### Disable compaction

Compaction on the target cluster must be disabled during migration to prevent conflicts with incoming replication data.

1. Dynamically disable compaction:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="0");
   ```

2. To prevent compaction from being re-enabled after a cluster restart, also add the following configuration to the FE configuration file **fe.conf**:

   ```Properties
   lake_compaction_max_tasks = 0
   ```

After migration is complete, re-enable compaction by removing the configuration from **fe.conf** and running:

```SQL
ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
```

#### Enable Legacy Compatibility for Replication

StarRocks may behave differently between versions, which can cause problems during cross-cluster data migration. You must enable Legacy Compatibility for the target cluster before migration and disable it after migration is completed.

1. Check whether Legacy Compatibility for Replication is already enabled:

   ```SQL
   ADMIN SHOW FRONTEND CONFIG LIKE 'enable_legacy_compatibility_for_replication';
   ```

   If `true` is returned, it is already enabled.

2. Dynamically enable Legacy Compatibility for Replication:

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="true");
   ```

3. To prevent this setting from being lost on cluster restart, also add the following to **fe.conf**:

   ```Properties
   enable_legacy_compatibility_for_replication = true
   ```

After migration is complete, remove the configuration from **fe.conf** and run:

```SQL
ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
```

#### Create source storage volumes on the target cluster

The migration tool identifies which storage volume each source table uses, and looks up a corresponding storage volume on the target cluster using the naming convention `src_<source_volume_name>`. You must pre-create these volumes before starting migration.

1. On the **source cluster**, list all storage volumes:

   ```SQL
   SHOW STORAGE VOLUMES;
   ```

2. For each storage volume used by the tables you plan to migrate, describe it to get its configuration:

   ```SQL
   DESCRIBE STORAGE VOLUME <volume_name>;
   ```

   Example output:

   ```
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | Name                | Type | IsDefault | Location                      | Params                   |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   | builtin_storage_vol | S3   | true      | s3://my-bucket                | {"aws.s3.region":"...",...} |
   +---------------------+------+-----------+-------------------------------+--------------------------+
   ```

3. On the **target cluster**, create a mirrored storage volume using the same object storage credentials, but with the name prefixed by `src_`:

   ```SQL
   CREATE STORAGE VOLUME src_<source_volume_name>
   TYPE = S3
   LOCATIONS = ("<same_location_as_source>")
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
   - Set `aws.s3.enable_partitioned_prefix` to `false` regardless of the source cluster's setting. The migration tool reads files using the source partition's full path directly, so partitioned prefix must not be applied to the mirrored volume.
   - Repeat this step for **each** unique storage volume used by the tables to be migrated. For example, if the source uses `builtin_storage_volume`, create `src_builtin_storage_volume` on the target cluster.
   - It is recommended to use temporary credentials (access key / secret key) for the source storage volume. These can be revoked after migration is complete.
   :::

#### Configure data migration parameters (Optional)

You can configure data migration operations using the following parameters. In most cases, the default values work well.

:::note
Increasing these values can accelerate migration but will also increase the load on the source cluster.
:::

**FE parameters** (dynamic, no restart needed):

| **Parameter**                         | **Default** | **Unit** | **Description**                                              |
| ------------------------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_max_parallel_table_count  | 100         | -        | Maximum number of concurrent table synchronization tasks. |
| replication_max_parallel_replica_count| 10240       | -        | Maximum number of tablet replicas allowed for concurrent synchronization. |
| replication_max_parallel_data_size_mb | 1048576     | MB       | Maximum data size allowed for concurrent synchronization. |
| replication_transaction_timeout_sec   | 86400       | Seconds  | Timeout for synchronization tasks. |

**BE/CN parameters** (dynamic, no restart needed):

| **Parameter**       | **Default** | **Unit** | **Description**                                              |
| ------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_threads | 0           | -        | Number of threads for executing synchronization tasks. `0` sets the thread count to 4× the number of CPU cores. |

## Step 1: Install the Tool

Install the migration tool on the server where the target cluster resides.

1. Download the binary package:

   ```Bash
   wget https://releases.starrocks.io/starrocks/starrocks-cluster-sync.tar.gz
   ```

2. Decompress the package:

   ```Bash
   tar -xvzf starrocks-cluster-sync.tar.gz
   ```

## Step 2: Configure the Tool

### Migration-related configuration

Navigate to the extracted folder and modify **conf/sync.properties**:

```Bash
cd starrocks-cluster-sync
vi conf/sync.properties
```

The file content is as follows:

```Properties
# If true, all tables will be synchronized only once, and the program will exit automatically after completion.
one_time_run_mode=false

source_fe_host=
source_fe_query_port=9030
source_cluster_user=root
source_cluster_password=
source_cluster_password_secret_key=
# source_cluster_token is NOT required for shared-data source clusters.
# Leave this empty or omit it.
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
# This is useful when the source cluster has multiple custom storage volumes but
# you want to consolidate all tables under one storage volume on the target cluster.
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

Parameter descriptions:

| **Parameter**                             | **Description**                                              |
| ----------------------------------------- | ------------------------------------------------------------ |
| one_time_run_mode                         | Whether to enable one-time synchronization mode. When enabled, the migration tool only performs a full synchronization and then exits. |
| source_fe_host                            | The IP address or FQDN of the source cluster's FE. |
| source_fe_query_port                      | The query port (`query_port`) of the source cluster's FE. |
| source_cluster_user                       | The username used to log in to the source cluster. This user must have the OPERATE privilege at the SYSTEM level. |
| source_cluster_password                   | The password for the source cluster user. |
| source_cluster_password_secret_key        | The secret key used to encrypt `source_cluster_password`. Leave empty (default) to use the password as plain text. To encrypt, run: `SELECT TO_BASE64(AES_ENCRYPT('<password>','<secret_key>'))`. |
| source_cluster_token                      | Token of the source cluster. **Not required for shared-data source clusters.** Leave this empty. Cluster tokens are only used for BE-to-BE snapshot authentication in shared-nothing source clusters; for shared-data clusters, files are read directly from object storage without any BE involvement. |
| target_fe_host                            | The IP address or FQDN of the target cluster's FE. |
| target_fe_query_port                      | The query port (`query_port`) of the target cluster's FE. |
| target_cluster_user                       | The username used to log in to the target cluster. This user must have the OPERATE privilege at the SYSTEM level. |
| target_cluster_password                   | The password for the target cluster user. |
| target_cluster_password_secret_key        | The secret key used to encrypt `target_cluster_password`. Same encryption mechanism as `source_cluster_password_secret_key`. |
| jdbc_connect_timeout_ms                   | JDBC connection timeout for FE queries. Default: `30000`. |
| jdbc_socket_timeout_ms                    | JDBC socket timeout for FE queries. Default: `60000`. |
| include_data_list                         | Databases and tables to migrate, comma-separated. Example: `db1, db2.tbl2, db3`. Takes priority over `exclude_data_list`. Leave empty to migrate all. |
| exclude_data_list                         | Databases and tables to exclude from migration, comma-separated. `include_data_list` takes priority. Leave empty to migrate all. |
| target_cluster_storage_volume             | The storage volume to use for new tables created on the target cluster. If empty, the target cluster's default storage volume is used. This is separate from the source storage volumes (which are specified by `src_<name>` volumes). |
| target_cluster_replication_num            | Number of replicas for tables created on the target cluster. `-1` uses the same replication number as the source. |
| target_cluster_max_disk_used_percent      | (Applies to shared-nothing targets only) Disk usage threshold for BE nodes. Migration stops if any BE exceeds this. Default: `80` (80%). |
| target_cluster_enable_persistent_index    | Whether to enable persistent index on the target cluster. Leave empty (default) to match the source cluster. |
| target_cluster_use_builtin_storage_volume_only | When `true`, all tables on the target cluster use `builtin_storage_volume`, ignoring the source cluster's storage_volume assignment. Default: `false`. |
| meta_job_interval_seconds                 | Interval (seconds) at which the tool retrieves metadata from source and target clusters. |
| meta_job_threads                          | Number of threads for metadata retrieval. |
| ddl_job_interval_seconds                  | Interval (seconds) at which the tool executes DDL statements on the target cluster. |
| ddl_job_batch_size                        | Batch size for executing DDL statements on the target cluster. |
| ddl_job_allow_drop_target_only            | Whether to delete databases or tables that exist only on the target cluster (not in the source). Default: `false`. |
| ddl_job_allow_drop_schema_change_table    | Whether to delete tables with inconsistent schemas between source and target. Default: `true`. The tool re-syncs them automatically. |
| ddl_job_allow_drop_inconsistent_partition | Whether to delete partitions with inconsistent data distribution. Default: `true`. The tool re-syncs them automatically. |
| ddl_job_allow_drop_partition_target_only  | Whether to delete partitions that were deleted in the source cluster. Default: `true`. |
| replication_job_interval_seconds          | Interval (seconds) at which the tool triggers data synchronization tasks. |
| replication_job_batch_size                | Batch size for triggering data synchronization tasks. |
| max_replication_data_size_per_job_in_gb   | Data size threshold per synchronization job. Unit: GB. Default: `1024`. |
| report_interval_seconds                   | Interval (seconds) at which the tool prints progress information. Default: `300`. |
| enable_bitmap_index_sync                  | Whether to synchronize Bitmap indexes. Default: `false`. |
| ddl_job_allow_drop_inconsistent_bitmap_index | Whether to delete inconsistent Bitmap indexes between source and target. Default: `true`. |
| ddl_job_allow_drop_bitmap_index_target_only | Whether to delete Bitmap indexes that were deleted in the source cluster. Default: `true`. |
| enable_materialized_view_sync             | Whether to synchronize materialized views. Default: `false`. |
| ddl_job_allow_drop_inconsistent_materialized_view | Whether to delete inconsistent materialized views. Default: `true`. |
| ddl_job_allow_drop_materialized_view_target_only | Whether to delete materialized views that were deleted in the source cluster. Default: `false`. |
| enable_view_sync                          | Whether to synchronize logical views. Default: `false`. |
| ddl_job_allow_drop_inconsistent_view      | Whether to delete inconsistent logical views. Default: `true`. |
| ddl_job_allow_drop_view_target_only       | Whether to delete logical views that were deleted in the source cluster. Default: `false`. |
| enable_table_property_sync                | Whether to synchronize table properties. Default: `false`. |

:::note
**Primary Key table persistent index**: When migrating between two shared-data clusters, the tool automatically converts `persistent_index_type = LOCAL` to `CLOUD_NATIVE` in the CREATE TABLE statement for Primary Key tables. No manual action is needed.
:::

### Storage volume mapping

When the migration tool creates a table on the target cluster, it determines the table's storage volume as follows (in order of precedence):

1. If `target_cluster_use_builtin_storage_volume_only = true`: use `builtin_storage_volume` for all tables.
2. If `target_cluster_storage_volume = <name>`: use the specified storage volume for all tables.
3. Otherwise (default): preserve the source table's storage volume name. Tables from different source storage volumes are created under the corresponding storage volumes on the target cluster, provided those storage volumes exist on the target.

This means you can use **multiple non-`src_`-prefixed storage volumes on the target cluster**. For example, if the source cluster has tables split across `ssd_volume` and `oss_volume`, you can pre-create both volumes on the target cluster, and each table will be placed in its corresponding volume after migration.

The `src_<name>` storage volumes serve a different purpose: they are used **only during migration** to give target CNs read access to the source cluster's object storage. After migration is complete, the `src_<name>` volumes are no longer needed and can be dropped.

### Network-related configuration (Optional)

During data migration, the migration tool needs to access **all** FE nodes of both the source and target clusters.

:::note
Unlike shared-nothing to shared-data migration, you do **not** need to configure network access from the target cluster to the source cluster's CN nodes, because data is transferred directly between object storage systems.
:::

You can obtain the FE network addresses by running on each cluster:

```SQL
-- FE nodes
SHOW FRONTENDS;
```

If FE nodes use private addresses (such as internal Kubernetes addresses) that are not reachable from outside the cluster, you need to add address mappings in **conf/hosts.properties**:

```Bash
cd starrocks-cluster-sync
vi conf/hosts.properties
```

Format:

```Properties
# <SOURCE/TARGET>_<host>=<mappedHost>[;<srcPort>:<dstPort>[,<srcPort>:<dstPort>...]]
```

:::note
`<host>` must exactly match the address shown in the `IP` column returned by `SHOW FRONTENDS`.
:::

Example: map the target cluster's internal Kubernetes FQDN to a reachable IP:

```Properties
TARGET_frontend-0.frontend.mynamespace.svc.cluster.local=10.1.2.1;9030:19030
```

## Step 3: Start the Migration Tool

After configuring the tool, start it to begin data migration:

```Bash
./bin/start.sh
```

:::note

- The migration tool regularly checks whether the target cluster's data lags behind the source cluster and initiates synchronization tasks when it does.
- If new data is constantly being loaded into the source cluster, synchronization continues until the target cluster is fully caught up.
- You can query tables on the target cluster during migration, but do not load new data into the target cluster — this may cause inconsistencies.
- The migration tool does not stop automatically. Manually confirm that migration is complete, then stop the tool.

:::

## View Migration Progress

### View migration tool logs

Check migration progress in the log file **log/sync.INFO.log**.

**View task progress** (search for `Sync job progress`):

![img](../_assets/data_migration_tool-1.png)

Key metrics:

| **Metric**  | **Description** |
| ----------- | --------------- |
| `total`     | Total number of all jobs in this migration run. |
| `ddlPending`| Number of pending DDL jobs. |
| `jobPending`| Number of pending data synchronization jobs. |
| `sent`      | Data synchronization jobs sent from the source cluster but not yet started. If this value keeps increasing, contact support. |
| `running`   | Number of currently running data synchronization jobs. |
| `finished`  | Number of completed data synchronization jobs. |
| `failed`    | Number of failed data synchronization jobs. Failed jobs are retried automatically; a large number of persistent failures warrants investigation. |
| `unknown`   | Number of jobs with unknown status. This should always be `0`. |

`Sync job progress` of 100% means all data synchronization is complete within the current check interval. If new data continues to be loaded into the source cluster, progress may decrease in the next interval — this is expected.

**View table migration progress** (search for `Sync table progress`):

![img](../_assets/data_migration_tool-2.png)

| **Metric**          | **Description** |
| ------------------- | --------------- |
| `finishedTableRatio`| Ratio of tables with at least one successful synchronization. |
| `expiredTableRatio` | Ratio of tables with expired data. |
| `total table`       | Total number of tables in this migration run. |
| `finished table`    | Number of tables with at least one successful synchronization. |
| `unfinished table`  | Number of tables with no successful synchronization yet. |
| `expired table`     | Number of tables with expired data. |

### View migration transaction status

The migration tool opens one transaction per table. Check the status with:

```SQL
SHOW PROC "/transactions/<db_name>/running";
```

### View partition data versions

Compare partition versions between source and target to verify migration status:

```SQL
SHOW PARTITIONS FROM <table_name>;
```

### View data volume

```SQL
SHOW DATA;
```

### View table row count

```SQL
SELECT
  TABLE_NAME,
  TABLE_ROWS
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

## After Migration

When `Sync job progress` has been stable at 100% and your business is ready to switch, complete the cutover as follows:

1. Stop writes to the source cluster.
2. Verify `Sync job progress` reaches and remains at 100% after writes stop.
3. Stop the migration tool.
4. Point your applications to the target cluster address.
5. Restore the source cluster's auto-vacuum setting:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_autovacuum_grace_period_minutes"="30");
   ```

6. Re-enable compaction on the target cluster. Remove `lake_compaction_max_tasks = 0` from **fe.conf** and run:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
   ```

7. Disable Legacy Compatibility for Replication on the target cluster. Remove `enable_legacy_compatibility_for_replication = true` from **fe.conf** and run:

   ```SQL
   ADMIN SET FRONTEND CONFIG("enable_legacy_compatibility_for_replication"="false");
   ```

## Limits

The following object types support synchronization (others are not supported):

- Databases
- Internal tables and their data
- Materialized view schemas and their building statements (materialized view data is not synchronized; if base tables are not migrated, the background refresh task will report errors)
- Logical views

Additional limits for shared-data to shared-data migration:

- The target cluster must be a shared-data cluster (v4.1 or later). Migration to a shared-nothing target is not supported.
- Each storage volume used by the source cluster's tables must have a corresponding `src_<volume_name>` storage volume pre-created on the target cluster.
