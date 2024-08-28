---
displayed_sidebar: docs
---

# Spill to disk

This topic describes how to spill intermediate computation results of large operators to local disks and object storage.

## Overview

For database systems that rely on in-memory computing for query execution, like StarRocks, they can consume substantial memory resources when processing queries with aggregate, sort, and join operators on a big dataset. When memory limits are reached, these queries are forcibly terminated due to out-of-memory (OOM).

However, there are still chances that you want certain memory-intensive tasks to be completed stably and performance is not your top priority, for example, building a materialized view, or performing a lightweight ETL with INSERT INTO SELECT. These tasks can easily exhaust your memory resources and thereby block other queries running in your cluster. Usually, to address this issue, you can only fine-tune these tasks individually, and rely on your resource isolation strategy to control the query concurrency. This could be particularly inconvenient and likely to fail under some extreme scenarios.

From StarRocks v3.0.1, StarRocks supports spilling the intermediate results of some memory-intensive operators to disks. With this feature, you can trade a tolerable drop in performance for a significant reduction in memory usage, thereby improving system availability.

Currently, StarRocks' spilling feature supports the following operators:

- Aggregate operators
- Sort operators
- Hash join (LEFT JOIN, RIGHT JOIN, FULL JOIN, OUTER JOIN, SEMI JOIN, and INNER JOIN) operators

## Enable intermediate result spilling

Follow these steps to enable intermediate result spilling:

1. Specify the local spill directory `spill_local_storage_dir`, which stores the spilled intermediate result on the local disk, in the BE configuration file **be.conf** or the CN configuration file **cn.conf**, and restart the cluster to allow the modification to take effect.

   ```Properties
   spill_local_storage_dir=/<dir_1>[;/<dir_2>]
   ```

   > **NOTE**
   >
   > - You can specify multiple `spill_local_storage_dir` by separating them with semicolons (`;`).
   > - In a production environment, we strongly recommend you use different disks for data storage and spilling. When intermediate results are spilled to disk, there could be a significant increase in both writing load and disk usage. If the same disk is used, this surge can impact other queries or tasks running in the cluster.

2. Execute the following statement to enable intermediate result spilling:

   ```SQL
   SET enable_spill = true;
   ```

3. Configure the mode of intermediate result spilling using the session variable `spill_mode`:

   ```SQL
   SET spill_mode = { "auto" | "force" };
   ```

   > **NOTE**
   >
   > Each time a query with spilling completes, StarRocks automatically clears the spilled data the query produces. If BE crashes before clearing the data, StarRocks clears it when the BE is restarted.

   | **Variable** | **Default** | **Description**                                              |
   | ------------ | ----------- | ------------------------------------------------------------ |
   | enable_spill | false       | Whether to enable intermediate result spilling. If it is set to `true`, StarRocks spills the intermediate results to disk to reduce the memory usage when processing aggregate, sort, or join operators in queries. |
   | spill_mode   | auto        | The execution mode of intermediate result spilling. Valid values:<ul><li>`auto`: Spilling is automatically triggered when the memory usage threshold is reached.</li><li>`force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.</li></ul>This variable takes effect only when the variable `enable_spill` is set to `true`. |

## [Preview] Spill intermediate result to object storage

From v3.3.0 onwards, StarRocks supports spilling intermediate results to object storage.

:::tip
Before enabling spilling to object storage, you must create a storage volume to define the object storage you want to use. For detailed instruction on creating a storage volume, see [CREATE STORAGE VOLUME](../../../sql-reference/sql-statements/cluster-management/storage_volume/CREATE_STORAGE_VOLUME.md).
:::

After you have enabled spilling in the previous step, you can further set these system variables to allow the intermediate results to be spilled to object storage:

```SQL
SET enable_spill_to_remote_storage = true;

-- Replace <storage_volume_name> with the name of the storage volume which you want to use.
SET spill_storage_volume = '<storage_volume_name>';
```

After spilling to object storage has been enabled, the intermediate results of queries that triggered spilling will be first stored in the local disks of the BE or CN nodes, and then the object storage if the capacity limit of the local disks is reached.

Please note that, if the storage volume you specified for `spill_storage_volume` does not exist, spilling to object storage will not be enabled.

## Limitations

- Not all OOM issues can be resolved by spilling. For example, StarRocks cannot release the memory used for expression evaluation.
- Usually, queries with spilling involved indicate a tenfold increase in query latency. We recommend you extend the query timeout for these queries by setting the session variable `query_timeout`.
- There is a significant performance drop in spilling to object storage compared to spilling to local disks.
- `spill_local_storage_dir` of each BE or CN node is shared among all queries running on the node. Currently, StarRocks does not support setting a size limit of spilled data to local disks individually for each query. Therefore, concurrent queries involved spilling may impact one another.
