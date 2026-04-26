---
displayed_sidebar: docs
---

# Operation and Maintenance

This topic provides answers to some questions related to operation and maintenance.

## Can the `trash` directory be cleaned up?

You can configure the FE parameter `catalog_trash_expire_second` to specify how long files stay in the FE `trash` directory (Default: 24 hours).

The BE parameter `trash_file_expire_time_sec` controls BE trash cleanup intervals (Default: 24 hours).

After a DROP TABLE or DROP DATABASE, the data first enters the FE trash and is kept for one day, during which it can be recovered with RECOVER. After that, it moves to the BE trash, which also keeps it for 24 hours.

## Do tablets have a primary–secondary relationship? If some replicas are missing, how does it impact queries?

If the table property `replicated_storage` is set to `true`, writes use a primary–secondary mechanism: data is written to the primary replica first and then synchronized to others. Multiple replicas generally do not have a major impact on query performance.

## Can I measure CPU and memory usage for a task through the monitoring interface?

You can check the `cpucostns` and `memcostbytes` fields in `fe.audit.log`.

## Creating a materialized view on a Unique Key table returns an error "The aggregation type of column[now_time] must be same as the aggregate type of base column in aggregate table". Does Unique Key table support materialized views?

The error indicates that the aggregation type of the materialized view must match that of the base table. For Unique Key tables, you can only materialized views to adjust their sort key order.
For example, if base table `tableA` has columns `k1`, `k2`, `k3`, with `k1` and `k2` as sort keys. While your queries contain the clause `WHERE k3=x` and need to be accelerated prefix index, you may create a materialized view that uses `k3` as the first column:

```SQL
CREATE MATERIALIZED VIEW k3_as_key AS
SELECT k3, k2, k1
FROM tableA;
```

## How can I get import volume metrics with precise timestamps? Is `query_latency` the metric for average response time?

Table-level import metrics can be obtained from: `http://user:password@fe_host:http_port/metrics?type=json&with_table_metrics=all`.

Cluster-level data can be retrieved from: `http://fe_host:http_port/api/show_data` (increment must be calculated manually).

`query_latency` provides percentile query response time.

## What is the difference between SHOW PROC '/backends' and SHOW BACKENDS?

`SHOW PROC '/backends'` retrieve metadata from the current FE and may lag. While `SHOW BACKENDS` retrieve metadata from the Leader FE and is authoritative.

## Does StarRocks have a timeout mechanism? Why do some client connections persist for a long time?

Yes. You can configure the system variable `wait_timeout` (Default: 8 hours) to adjust the connection timeout.

Example:

```SQL
SET GLOBAL wait_timeout = 3600;
```

## Can GRANT authorize multiple tables in one statement?

No. Statements like `GRANT <priv> on db1.tb1, db1.tb2` are not supported.

## Can I revoke privileges on a specific table when the ALL TABLES privilege is granted?

Subset revocation is not supported. It is recommended to grant privileges at the database or table level.

## Does StarRocks support table-level and row-level privileges?

Table-level access control is supported. Row- and column-level access controls are not supported in the Open-source Edition.

## If a Primary Key table is not partitioned, does hot–cold data separation still work?

No. Hot–cold separation is based on partitions.

## Can I change the data storage path if the data was mistakenly placed in the root directory?

Yes.  Update `storage_root_path` in `be.conf` to add a new disk, and use semicolons to separate paths.

## How do I check the StarRocks version of FE?

Run `SHOW FRONTENDS;` and check the `Version` field.

## Does StarRocks support DELETE with nested subqueries?

From v2.3 onwards, Primary Key tables supports full DELETE WHERE syntax.
See [Reference - DELETE](https://docs.starrocks.io/zh/docs/sql-reference/sql-statements/table_bucket_part_index/DELETE/) for details.

## For dynamic partitions, if I don't want old partitions automatically cleaned, can I simply omit dynamic_partition.start?

No. Set it to a very large value.

## If a BE machine has faulty memory and needs maintenance, what should be done?

[Decommission](../sql-reference/sql-statements/cluster-management/nodes_processes/ALTER_SYSTEM.md#be) the BE. After repair, add it back to the cluster.

## Can I enforce all future partitions to use SSD by default?

No. Default is HDD. Manual configuration is required.

## After adding new BEs, tablets automatically rebalanced. Can I decommission old BEs immediately?

Yes. You do not need to wait until the rebalancing completed. Up to two nodes can be decommissioned at once.

## Will adding new BEs and removing old ones affect performance?

Rebalancing happens automatically and should not affect normal operations. It is recommended to remove nodes one at a time.

## How to replace six BE nodes with six new ones?

Add six new BE nodes, and then decommission the old ones one by one.

## Why doesn't unhealthy tablet count decrease after decommissioning a node?

Check for single-replica tables. Other repairs may be blocked if they continuously retry.

## What does this BE log mean? "tcmalloc: large alloc xxxxxxxx bytes"

A large memory allocation request occurred, often caused by large queries. Check the corresponding `query_id` in `be.INFO` to locate the SQL.

## Will tablet migration after adding nodes cause disk I/O fluctuations?

Yes, temporary I/O fluctuation is expected during balancing.

## What are the recommended data migration methods for deployments on the cloud?

- To migrate a single table, you can:
  - Create a StarRocks external table, and then load the data from it using INSERT INTO SELECT.
  - Read data from the source BE using a Spark-connector-based programme, and load the data into the target BE using encapsulated STREAM LOAD.
- To migrate multiple tables, you can use Backup and Restore. First, back up the data from the source cluster to the remote storage. Then, restore the data from the remote storage to the target cluster.
- If your cluster uses HDFS as the remote storage, you can first use `distcp` to migrate the data files, and then use Broker Load to load the data into the target cluster.

## How can I resolve the error "failed to create task: disk ... exceed limit usage"?

The disk is full. Scale up the storage or clean trash.

## FE logs show "tablet migrate failed". How can I solve this?

It is likely because the `storage_root_path` is HDD and the table property `storage_medium` is set to `SSD`. You can set the table property to `HDD`:

```SQL
ALTER TABLE db.table MODIFY PARTITION (*) SET("storage_medium"="HDD");
```

## Can I migrate FE to another machine by copying metadata only?

No. Use the recommended method: add a new node, and then remove the old one.

## To disks on one BE have uneven usage (500 GB 99%, 2 TB 20%). Why is balancing not happening?

Balancing assumes equal-sized disks. Use disks of the same size to ensure even distribution.

## If `max_backend_down_time_second` is set to `3600`, does it mean that I must recover the failed BE within one hour?

If the downtime of the BE exceeds this configuration, FE will replenish replicas on other BE. When the failed BE is added back to the cluster, large rebalancing costs may occur.

## Is there an IDE tool to export table structures and views from the production environment?

Yes. See [olapdb-tool](https://github.com/Astralidea/olapdb-tool).

## Can multiple system variables be set in a single SQL (for example, timeout and parallelism)?

Yes.

Example:

```SQL
SELECT /*+ SET_VAR(query_timeout=1, is_report_success=true, parallel_fragment_exec_instance_num=2) */ COUNT(1)
FROM table;
```

## Do VARCHAR columns in sort keys follow the 36-byte prefix limit?

VARCHAR columns are truncated based on actual length. Only the first column gets a short-key index. Place VARCHAR sort keys in the third position if possible.

## BE fails to start with "while lock file" error. How can I deal with it?

BE process is still running. Kill the daemon process and restart.

## Do clients need to explicitly connect to observer FEs for read-only queries?

No. Write requests automatically route to the leader FE; observers serve read-only queries.

## FE exits with `LOG_FILE_NOT_FOUND` caused by too many open files. How can I solve this?

Check OS file descriptor limits by running `cat /proc/$pid/limits`, and run `lsof -n -p $fe_pid>/tmp/fe_fd.txt` to inspect the file descriptor in use.

## Are there limits on partition and bucket numbers?

- For partitions, the default limit `4096` (configurable via the FE configuration `max_partitions_in_one_batch`).
- For buckets, there is no limit. The recommended size for each bucket is 1 GB.

## How to manually switch FE leader?

Stop the current Leader, and a new leader will be elected automatically.

## How to replace three FE nodes (1 Leader and 2 Follower) with new machines?

1. Add 2 new Followers to the cluster.
2. Remove 1 old Follower.
3. Add one last new Follower.
4. Remove the remaining old Follower.
5. Remove the old Leader.

## Dynamic partition creation not working as expected. Why?

Dynamic partition check runs every 10 minutes. This behavior is controlled by the FE configuration `dynamic_partition_check_interval_seconds`.

## FE logs frequently show "connect processor exception because，java.io.IOException: Connection reset by peer". Why?

It is likely because client-side disconnects, connection pool drops, or network issues. Check OS backlog metrics and network stability.

Check if the following metrics have changed:

```Bash
netstat -s | grep -i LISTEN
netstat -s | grep TCPBacklogDrop
cat /proc/sys/net/core/somaxconn
```

## After I execute TRUNCATE statements, when will the storage space to be released?

Immediately.

## How to check whether bucket distribution is balanced?

Run the following command:

```SQL
SHOW TABLET FROM db.table PARTITION (<partition_name>);
```

## How to solve the error "Fail to get master client from cache"?

It is an FE–BE communication failure. Check IP and port connectivity.

## How to migrate StarRocks when IP changes?

FQDN-mode deployment is recommended.

## Can a non-partitioned table be converted to a partitioned table?

No. You can create a new partitioned table and use INSERT INTO SELECT to migrate the data.

## Can I query historical SQL execution? Is there an audit log?

Yes. See `fe.audit.log`.

## How to convert a non-partitioned table to SSD storage?

Run the following SQL:

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET ("storage_medium"="SSD");
```

## After executing ALTER TABLE ADD COLUMN, queries against `information_schema.COLUMNS` show delay. Is this normal?

Yes. ALTER operations are asynchronous. Check progress with `SHOW ALTER COLUMN`.

## If retention for a dynamic partition table is changed from 366 to 732 days, can historical partitions be auto-created?

Follow these steps:

1. Disable dynamic partitions.

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "false");
   ```

2. Manually add partitions.

   ```SQL
   ALTER TABLE db.tbl ADD PARTITIONS START ("2019-01-01") END ("2019-12-31") EVERY (interval 1 day);
   ```

3. Re-enable dynamic partitions.

   ```SQL
   ALTER TABLE db.tbl SET  ("dynamic_partition.enable" = "true"); 
   ```

## Can Routine Load tasks be monitored and alerted when switching from Running to Paused?

Yes. StarRocks supports monitoring metrics for Routine Load tasks and can be connected to alert systems.

## How to diagnose unhealthy replicas?

Run the following statements to identify UnhealthyTablets:

```SQL
SHOW PROC '/statistic'
SHOW PROC '/statistic/<db_id>'
```

Then, analyze UnhealthyTablets with `SHOW TABLET tablet_id`.

If the result shows that two replicas have consistent data but one replica has inconsistent data—meaning two out of three replicas completed the write successfully—this is considered a successful write. You can then check whether the tablets in UnhealthyTablets are fixed. If they are fixed, it indicates an issue. If the status is changing, you can adjust the loading frequency for the corresponding table.

## Error: "SyntaxErrorException: Reach limit of connections". How to troubleshoot?

Increase per-user limits by running the following command:

```SQL
ALTER USER 'jack' SET PROPERTIES ('max_user_connections'='1000');
```

Also check load balancers and idle connection accumulation (`wait_timeout`).

## How does XFS and ext4 affect QPS?

StarRocks typically performs better with XFS.

## How long before a BE is considered down and tablet migration begins?

1. When Heartbeat (default: every 5 seconds) failed 3 times, the BE is marked as not alive.
2. After that, there is an intended delay before executing Clone, which is 60 seconds.
3. Then, the replica clone starts.

If BE recovers later, its replicas are deleted.

## Tablet scheduling on new nodes is slow (only 100 at a time). How to adjust?

Tune the following FE configurations:

```SQL
ADMIN SET FRONTEND CONFIG ("schedule_slot_num_per_path"="8");
ADMIN SET FRONTEND CONFIG ("max_scheduling_tablets"="1000");
ADMIN SET FRONTEND CONFIG ("max_balancing_tablets"="1000");
```

## Is BACKUP operation serial? It seems only one HDFS directory is changing

BACKUP operations are parallel, but upload to HDFS uses a single worker, which is controlled by the BE configuration `upload_worker_count`. Adjust it with caution because it might affect the disk I/O and network I/O.

## How to solve the FE OOM error "OutOfMemoryError: GC overhead limit exceeded"?

Increase FE JVM memory.

## How to solve the error "Cannot truncate a file by broker"?

1. Check broker logs for error messages.
2. Check BE warning logs for error "remote file checksum is invalid. remote:**** local: *****".
3. Search the remote number the broker `apache_hdfs_broker.log` for error "receive a check path request, request detail", and identify the duplicate files.
4. Remove or rename problematic remote files and retry.

## How to verify that disk removal has completed?

Run `SHOW PROC '/statistic'` and ensure `UnhealthyTablet` count is zero.

## How to modify replica count for historical partitions?

Run the following command:

```SQL
ALTER TABLE db.tbl MODIFY PARTITION (*) SET("replication_num"="3");
```

## For a three-replica table, if the disk for a BE node is damaged, will replicas automatically recover to maintain three copies?

Yes, if enough BE nodes are available.

## How to troubleshoot if the error "reach limit connections" continues even after increasing user limits?

Check load balancers (ProxySQL, F5), idle connection accumulation, and reduce `wait_timeout` to 2–4 hours.

## If FE metadata is lost, is all cluster metadata lost?

Metadata resides in FE. With only one FE, it's unrecoverable. With multiple FEs, you can re-add the failed node and metadata will be replicated.

## How to solve the loading error "INTERNAL_ERROR, FE leader shows NullPointerException"?

Add JVM option `-XX:-OmitStackTraceInFastThrow` and restart FE to get full stack trace.

## How to solve the error "The partition column could not be aggregated column" when setting the partition column for a Primary Key table?

The partition columns must be key columns.
