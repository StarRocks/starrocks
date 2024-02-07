---
displayed_sidebar: "English"
---

# Cross-cluster Data Migration Tool

The StarRocks Cross-cluster Data Migration Tool is provided by StarRocks Community. You can use this tool to easily migrate data from the source cluster to the target cluster.

:::note

- The StarRocks Cross-cluster Data Migration Tool only supports migrating data from a shared-nothing cluster to either another shared-nothing cluster or a shared-data cluster.
- The StarRocks version of the target cluster must be v3.1.8, v3.2.3, or later.

:::

## Preparations

The following preparations must be performed on the target cluster for data migration.

### Disable Compaction

If the target cluster for data migration is a shared-data cluster, you need to manually disable Compaction before starting the data migration and re-enable it after the data migration is completed.

1. You can check whether Compaction is enabled by using the following statement:

   ```SQL
   ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
   ```

   If `0` is returned, it indicates that Compaction is disabled.

2. Dynamically disable Compaction:

   ```SQL
   ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="0");
   ```

3. To prevent Compaction from automatically enabling during the data migration process in case of cluster restart, you also need to add the following configuration item in the FE configuration file **fe.conf**:

   ```Properties
   lake_compaction_max_tasks = 0
   ```

After the data migration is completed, you need to remove the configuration `lake_compaction_max_tasks = 0` from the configuration file, and dynamically enable Compaction using the following statement:

```SQL
ADMIN SET FRONTEND CONFIG("lake_compaction_max_tasks"="-1");
```

### Configure Data Migration (Optional)

You can configure data migration operations using the following FE and BE parameters. In most cases, the default configuration can meet your needs. If you wish to use the default configuration, you can skip this step.

:::note

Please note that increasing the values of the following configuration items can accelerate migration but will also increase the load pressure on the source cluster.

:::

#### FE Parameters

The following FE parameters are dynamic configuration items. Refer to [Configure FE Dynamic Parameters](../administration/FE_configuration.md#configure-fe-dynamic-parameters) on how to modify them.

| **Parameter**                         | **Default** | **Unit** | **Description**                                              |
| ------------------------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_max_parallel_table_count  | 100         | -        | The maximum number of concurrent data synchronization tasks allowed. StarRocks creates one synchronization task for each table. |
| replication_max_parallel_data_size_mb | 10240       | MB       | The maximum size of data allowed for concurrent synchronization. |
| replication_transaction_timeout_sec   | 3600        | 秒       | The timeout duration for synchronization tasks.              |

#### BE Parameters

The following BE parameter is a dynamic configuration item. Refer to [Configure BE Dynamic Parameters](../administration/BE_configuration.md#configure-be-dynamic-parameters) on how to modify it.

| **Parameter**       | **Default** | **Unit** | **Description**                                              |
| ------------------- | ----------- | -------- | ------------------------------------------------------------ |
| replication_threads | 0           | -        | The number of threads for executing synchronization tasks. `0` indicates setting the number of threads to the number of CPU cores on the machine where the BE resides. |

## Step 1: Install the Tool

It is recommended to install the migration tool on the server where the target cluster resides.

1. Launch a terminal, and download the binary package of the tool.

   ```Bash
   wget https://releases.starrocks.io/starrocks/starrocks-cluster-sync.tar.gz
   ```

2. Decompress the package.

   ```Bash
   tar -xvzf starrocks-cluster-sync.tar.gz
   ```

## Step 2: Configure the Tool

Navigate to the extracted folder and modify the configuration file **conf/sync.properties**.

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
source_cluster_token=

target_fe_host=
target_fe_query_port=9030
target_cluster_user=root
target_cluster_password=

meta_job_interval_seconds=180
ddl_job_interval_seconds=15
ddl_job_batch_size=10
ddl_job_allow_drop_target_only=false
ddl_job_allow_drop_schema_change_table=true
ddl_job_allow_drop_inconsistent_partition=true
replication_job_interval_seconds=15
replication_job_batch_size=10

# Comma-separated list of database names or table names like <db_name> or <db_name.table_name>
# example: db1,db2.tbl2,db3
# Effective order: 1. include 2. exclude
include_data_list=
exclude_data_list=
```

The description of the parameters is as follows:

| **Parameter**                             | **Description**                                              |
| ----------------------------------------- | ------------------------------------------------------------ |
| one_time_run_mode                         | Whether to enable one-time synchronization mode. When one-time synchronization mode is enabled, the migration tool only performs full synchronization instead of incremental synchronization. |
| source_fe_host                            | The IP address or FQDN (Fully Qualified Domain Name) of the source cluster's FE. |
| source_fe_query_port                      | The query port (`query_port`) of the source cluster's FE.    |
| source_cluster_user                       | The username used to log in to the source cluster. This user must be granted the OPERATE privilege on the SYSTEM level. |
| source_cluster_password                   | The user password used to log in to the source cluster.      |
| source_cluster_token                      | Token of the source cluster. For information on how to obtain the cluster token, refer to [Obtain Cluster Token](#obtain-cluster-token) below. |
| target_fe_host                            | The IP address or FQDN (Fully Qualified Domain Name) of the target cluster's FE. |
| target_fe_query_port                      | The query port (`query_port`) of the target cluster's FE.    |
| target_cluster_user                       | The username used to log in to the target cluster. This user must be granted the OPERATE privilege on the SYSTEM level. |
| target_cluster_password                   | The user password used to log in to the target cluster.      |
| meta_job_interval_seconds                 | The interval, in seconds, at which the migration tool retrieves metadata from the source and target clusters. You can use the default value for this item. |
| ddl_job_interval_seconds                  | The interval, in seconds, at which the migration tool executes DDL statements on the target cluster. You can use the default value for this item. |
| ddl_job_batch_size                        | The batch size for executing DDL statements on the target cluster. You can use the default value for this item. |
| ddl_job_allow_drop_target_only            | Whether to allow the migration tool to delete databases, tables, or partitions that exist only in the target cluster but not in the source cluster. The default is `false`, which means they will not be deleted. You can use the default value for this item. |
| ddl_job_allow_drop_schema_change_table    | Whether to allow the migration tool to delete tables with inconsistent schemas between the source and target clusters. The default is `true`, meaning they will be deleted. You can use the default value for this item. The migration tool will automatically synchronize the deleted tables during the migration. |
| ddl_job_allow_drop_inconsistent_partition | Whether to allow the migration tool to delete partitions with inconsistent data distribution between the source and target clusters. The default is `true`, meaning they will be deleted. You can use the default value for this item. The migration tool will automatically synchronize the deleted partitions during the migration. |
| replication_job_interval_seconds          | The interval, in seconds, at which the migration tool triggers data synchronization tasks. You can use the default value for this item. |
| replication_job_batch_size                | The batch size at which the migration tool triggers data synchronization tasks. You can use the default value for this item. |
| include_data_list                         | The databases and tables that need to be migrated, with multiple objects separated by commas (`,`). For example: `db1, db2.tbl2, db3`. This item takes effect prior to `exclude_data_list`. If you want to migrate all databases and tables in the cluster, you do not need to configure this item. |
| exclude_data_list                         | The databases and tables that do not need to be migrated, with multiple objects separated by commas (`,`). For example: `db1, db2.tbl2, db3`. `include_data_list` takes effect prior to this item. If you want to migrate all databases and tables in the cluster, you do not need to configure this item. |

### Obtain Cluster Token

You can obtain the cluster token either through the HTTP port of the FE or from the metadata of the FE node.

#### Obtain Cluster Token through the FE HTTP Port

Run the following command:

```Bash
curl -v http://<fe_host>:<fe_http_port>/check
```

- `fe_host`: The IP address or FQDN (Fully Qualified Domain Name) of the cluster's FE.
- `fe_http_port`: The HTTP port of the cluster's FE.

Output:

```Plain
* About to connect() to xxx.xx.xxx.xx port 8030 (#0)
*   Trying xxx.xx.xxx.xx...
* Connected to xxx.xx.xxx.xx (xxx.xx.xxx.xx) port 8030 (#0)
> GET /check HTTP/1.1
> User-Agent: curl/7.29.0
> Host: xxx.xx.xxx.xx:8030
> Accept: */*
> 
< HTTP/1.1 200 OK
< content-length: 0
< cluster_id: yyyyyyyyyyy
< content-type: text/html
< token: wwwwwwww-xxxx-yyyy-zzzz-uuuuuuuuuu
< connection: keep-alive
< 
* Connection #0 to host xxx.xx.xxx.xx left intact
```

The `token` field represents the token of the current cluster.

#### Obtain Cluster Token from FE Metadata

Log in to the server where the FE node is located and run the following command:

```Bash
cat fe/meta/image/VERSION | grep token
```

Output:

```Properties
token=wwwwwwww-xxxx-yyyy-zzzz-uuuuuuuuuu
```

## Step 3: Start the Migration Tool

After configuring the tool, start the migration tool to initiate the data migration process.

```Bash
./bin/start.sh
```

:::note

- Make sure that the BE nodes of the source and target clusters can properly communicate via the network.
- During runtime, the migration tool regularly checks whether the data in the target cluster is lagging behind the source cluster. If there is a lag, it initiates data migration tasks.
- If new data is constantly loaded into the source cluster, data synchronization will continue until the data in the target cluster is consistent with that in the source cluster.
- You can query tables in the target cluster during migration, but do not load new data into the tables, as it may result in inconsistencies between the data in the target cluster and the source cluster. Currently, the migration tool does not forbid data loading into the target cluster during migration.
- Note that data migration does not automatically terminate. You need to manually check and confirm the completion of migration and then stop the migration tool.

:::

## View Migration Progress

### View Migration Tool logs

You can check the migration progress through the migration tool log **log/sync.INFO.log**.

Example:

![img](../assets/data_migration_tool-1.png)

The important metrics are as follows:

- `Sync progress`: The progress of data migration. The migration tool regularly checks whether the data in the target cluster is lagging behind the source cluster. Therefore, a progress of 100% only means that the data synchronization is completed within the current check interval. If new data continues to be loaded into the source cluster, the progress may decrease in the next check interval.
- `total`: The total number of all types of jobs in this migration operation.
- `ddlPending`: The number of DDL jobs pending to be executed.
- `jobPending`: The number of pending data synchronization jobs to be executed.
- `sent`: The number of data synchronization jobs sent from the source cluster but not yet started. Theoretically, this value should not be too large. If the value keeps increasing, please contact our engineers.
- `running`: The number of data synchronization jobs that are currently running.
- `finished`: The number of data synchronization jobs that are finished.
- `failed`: The number of failed data synchronization jobs. Failed data synchronization jobs will be resent. Therefore, in most cases, you can ignore this metric. If this value is significantly large, please contact our engineers.
- `unknown`: The number of jobs with an unknown status. Theoretically, this value should always be `0`. If this value is not `0`, please contact our engineers.

### View Migration Transaction Status

The migration tool opens a transaction for each table. You can view the status of the migration for a table by checking the status of its corresponding transaction.

```SQL
SHOW PROC "/transactions/<db_name>/running";
```

`<db_name>` is the name of the database where the table is located.

### View Partition Data Versions

You can compare the data versions of the corresponding partitions in the source and target clusters to view the migration status of that partition.

```SQL
SHOW PARTITIONS FROM <table_name>;
```

`<table_name>` is the name of the table to which the partition belongs.

### View Data Volume

You can compare the data volumes in the source and target clusters to view the migration status.

```SQL
SHOW DATA;
```

### View Table Row Count

You can compare the row counts of tables in the source and target clusters to view the migration status of each table.

```SQL
SELECT 
  TABLE_NAME, 
  TABLE_ROWS 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE' 
ORDER BY TABLE_NAME;
```
