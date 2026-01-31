---
displayed_sidebar: docs
---

# Shared-data

This topic provides answers to some frequently asked questions about shared-data clusters.

## Why does table creation fail?

Check the BE log (`be.INFO`) to identify the exact cause. Common causes include:

- Misconfigured object storage settings (for example, `aws_s3_path`, `endpoint`, `authentication`).
- Object storage service instability or exceptions.

Other errors:

Error message: "Error 1064 (HY000): Unexpected exception: Failed to create shards. INVALID_ARGUMENT: shard info cannot be empty"

Cause: This often caused when automatic bucket inference is used while no CN or BE nodes are alive. This issues is fixed in v3.2.

## Why does table creation take too long?

Excessive bucket numbers (especially in partitioned tables) cause StarRocks to create many tablets. The system needs to write a tablet metadata file for each tablet in the object storage, whose high latency can drastically increase total creation time. You may consider:

- Reducing the number of buckets.
- Increasing the tablet creation thread pool size via the BE configuration `create_tablet_worker_count`.
- Checking and troubleshooting high write latency in object storage.

## Why is data in object storage not cleaned up after dropping a table?

StarRocks supports two DROP TABLE modes:

- `DROP TABLE xxx`: moves table metadata to FE recycle bin (data is not deleted).
- `DROP TABLE xxx FORCE`: immediately deletes table metadata and data.

If cleanup fails, check:

- Whether `DROP TABLE xxx FORCE` was used.
- Whether recycle bin retention parameters are set too high. Parameters include:
  - FE configuration `catalog_trash_expire_second`
  - BE configuration `trash_file_expire_time_sec`
- FE logs for deletion errors (for example, RPC timeout). Increase RPC timeout if needed.

## How can I find the storage path of table data in object storage?

Run the following command to get the storage path.

```SQL
SHOW PROC '/dbs/<database_name>';
```

Example:

```SQL
mysql> SHOW PROC '/dbs/load_benchmark';
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
| TableId | TableName   | IndexNum | PartitionColumnName | PartitionNum | State  | Type         | LastConsistencyCheckTime | ReplicaCount | PartitionType | StoragePath                                                                                                  |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
| 17152   | store_sales | 1        | NULL                | 1            | NORMAL | CLOUD_NATIVE | NULL                     | 64           | UNPARTITIONED | s3://starrocks-common/xxxxxxxxx-xxxx_load_benchmark-1699408425544/5ce4ee2c-98ba-470c-afb3-8d0bf4795e48/17152 |
+---------+-------------+----------+---------------------+--------------+--------+--------------+--------------------------+--------------+---------------+--------------------------------------------------------------------------------------------------------------+
1 row in set (0.18 sec)
```

In versions earlier than v3.1.4, table data is scattered under a single directory.

From v3.1.4 onwards, data is organized by partition. The same command displays the table root path, which now contains subdirectories named after Partition IDs, and each partition directory holds sub-directories `data/` (segment data files) and `meta/` (tablet metadata files).

## Why are queries slow in shared-data clusters?

Common causes include:

- Cache miss.
- Insufficient compaction, causing too many small segment files and thus excessive I/O.
- Bad parallelism (for example, too few tablets).
- Improper `datacache.partition_duration` settings, causing caching failures.

You need to analyze the Query Profile first to identify the root cause.

### Cache miss

In shared-data clusters, data is stored remotely, so Data Cache is crucial. If queries become unexpectedly slow, check Query Profile metrics such as `CompressedBytesReadRemote` and `IOTimeRemote`.

Cache misses may be caused by:

- Data Cache disabled during table creation.
- Insufficient local cache space.
- Tablet migration due to elastic scaling.
- Improper `datacache.partition_duration` settings preventing caching.

### Insufficient compaction

Without adequate compaction, many historical data versions remain, increasing the number of segment files accessed during queries. This increases I/O and slows down queries.

You can diagnose insufficient compaction by:

- Checking Compaction Score for relevant partitions.

  Compaction Score should remain below ~10. Excessively high Compaction Scores often indicate compaction failures.

- Reviewing Query Profile metrics such as `SegmentsReadCount`.

  If Segment counts are high, compaction may be lagging or stuck.

### Improper tablet settings

Tablets distribute data across Compute Nodes. Poor bucketing or skewed bucket keys can cause queries to run on only a subset of nodes. 

Recommendations:

- Choose bucket columns that ensure balanced distribution.
- Set reasonable bucket numbers (formula: `total data size / (1–5 GB)`).

### Improper `datacache.partition_duration` settings

If this value is set to too small, data from “cold” partitions may not be cached, causing repeated remote reads. In Query Profile, if `CompressedBytesReadRemote` or `IOCountRemote` is non-zero, this may be the reason. Tune `datacache.partition_duration` accordingly.

## Why do all queries under a warehouse time out with errors like “Timeout was reached” or “Deadline Exceeded”?

Check whether the Compute Nodes under the warehouse can access object storage endpoint.

## How to retrieve tablet metadata in shared-data clusters?

1. Obtain visible version by running:

   ```SQL
   SHOW PARTITIONS FROM <table_name>`
   ```

2. Execute the following statement to retrieve tablet metadata:

   ```SQL
   admin execute on <backend_id> 
   'System.print(StorageEngine.get_lake_tablet_metadata_json(<tablet_id>, <version>))'
   ```

## Why is loading slow during high-frequency ingestion?

StarRocks serializes transaction commit, so high ingestion rates may hit limits.

Monitor the following aspects:

- **Loading queue**: If loading queue is full, you can increase I/O worker threads.
- **Publish Version latency**: High publish times cause ingestion delays.

## How does compaction work in shared-data clusters, and why does it get stuck?

Key behaviors include:

- Compaction is scheduled by FE and executed by CN.
- Each compaction produces a new version and goes through the process of Write, Commit, and Publish.
- FE does not track runtime compaction tasks; stale tasks on BE may block new ones.

To clean up stuck compaction tasks, follow these steps:

1. Check version information of the partition, and compare `CompactVersion` and `VisibleVersion`.

   ```SQL
   SHOW PARTITIONS FROM <table_name>
   ```

2. Check compaction task status.

   ```SQL
   SHOW PROC '/compactions'
   ```

   ```SQL
   SELECT * FROM information_schema.be_cloud_native_compactions WHERE TXN_ID = <TxnID>
   ```

3. Cancel expired tasks.

   a. Disable compaction and migration.

      ```SQL
      ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "0");
      ADMIN SET FRONTEND CONFIG ('tablet_sched_disable_balance' = 'true');
      ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
      ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched_disable_balance';
      ```

   b. Restart all BE nodes

   c. Verify all compactions have failed.

      ```SQL
      SHOW PROC '/compactions'
      ```

   d. Re-enable compaction & migration.

      ```SQL
      ADMIN SET FRONTEND CONFIG ("lake_compaction_max_tasks" = "-1");
      ADMIN SET FRONTEND CONFIG ('tablet_sched_disable_balance' = 'false');
      ADMIN SHOW FRONTEND CONFIG LIKE 'lake_compaction_max_tasks';
      ADMIN SHOW FRONTEND CONFIG LIKE 'tablet_sched_disable_balance';
      ```

## Why is compaction slow in Kubernetes shared-data clusters?

If ingestion happens in one warehouse but compaction runs in another (for example, `default_warehouse`), compaction must pull data across warehouses with no cache, slowing it down.

Solution:

- Set BE configuration `lake_enable_vertical_compaction_fill_data_cache` to `true`.
- Perform writes in the same warehouse as compaction.

## Why does storage usage appear inflated in shared-data clusters?

Storage usage on object storage includes all historical versions, while `SHOW DATA` outputs only reflect the latest version.

However, if the difference is excessively large, it could be caused by the following issues:

- Compaction may be lagging.
- Vacuum tasks may be backlogged (check queue size).

You may consider tuning compaction or vacuum thread pools if necessary.

## Why do big queries fail with “meta does not exist?

The query may be using a data version already compacted and vacuumed.

To resolve this, you can increase file retention by modifying the FE configuration `lake_autovacuum_grace_period_minutes`, and then retry the query.

## What causes excessive small files in object storage?

Excessive number of small files may cause performance degradation. Common causes include:

- Too many buckets caused by improper bucket settings.
- High ingestion frequency causing very small individual loads.

Compaction will eventually merge small files, but tuning bucket count and batching ingestion helps prevent performance degradation.
