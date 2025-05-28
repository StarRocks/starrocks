# Schedule Compute Resource using Compute Replicas

StarRocks supports enabling multiple compute replicas for a warehouse.

In a shared-data cluster, data reliability is guaranteed by the object storage service provided by the cloud vendor. From v3.5.0 onwards, StarRocks further ensures cluster performance reliability with the support of Compute Replica when creating a Warehouse. You can enable Compute Replica for the Warehouse to allow the Warehouse to cache and warm up the latest metadata and data files, and thereby ensure cluster performance reliability.

The Multiple Compute Replica feature offers the following benefits:
- **Multiple available compute units**: Increasing the concurrency of queries against a single tablet
- **Cache replication**: Improving the performance stability in face of Compute Node failures
- **Cache warmup**: Lowering the query latency during node scale-in

## Enable Multiple Compute Replica

To enable Multiple Compute Replica, you must specify the corresponding properties for the warehouse:

- `compute_replica`
- `replication_type`
- `warmup_level`

You can either specify them while creating the warehouse:

```SQL
CREATE WAREHOUSE <warehouse_name>
PROPERTIES(
    "compute_replica" = "", 
    "replication_type" = "", 
    "warmup_level" = ""
);
```

Or modify them for an existing warehouse:

```SQL
ALTER WAREHOUSE <warehouse_name>
SET(
    "compute_replica" = "", 
    "replication_type" = "", 
    "warmup_level" = ""
);
```

### Properties

#### `compute_replica`

- **Default**: 1
- **Description**: Number of compute replicas.

:::note
**Related Configurations**

The maximum value of `compute_replica` is limited by the FE dynamic configuration item `lake_warehouse_max_compute_replica`. The default value of `lake_warehouse_max_compute_replica` is `3`.
:::

#### `replication_type`

- **Default**: NONE
- **Description**: Cache replication type. When data is being loaded into a CN node, the data of the source CN will be replicated to the caches of other CN nodes according to the replication type and the number of replicas. Valid values:
  - `NONE` (Default): The system will not replicate the data in the source CN node to the caches of other CN nodes.
  - `SYNC`: The system will synchronously replicate the data from the source CN node to the caches of other CN nodes. Under normal circumstances, when the loading transaction succeeds, the new data will be loaded into all compute replicas.
  - `ASYNC`: The system will asynchronously replicate the data from the source CN node to the caches of other CN nodes. When the loading transaction succeeds, the new data will be loaded into at least one compute replica. The replication in other compute replicas will be executed in background.

:::note
**Related Configurations**

- The timeout duration for replication is defined by the CN dynamic configuration item `starlet_cache_replication_timeout_ms` (Unit: Milliseconds). The default value of `starlet_cache_replication_timeout_ms` is `5000`.
- The thread number for background replication (only effective for `ASYNC` replication type) is defined by the CN dynamic configuration item `starlet_cache_thread_num`. The default value of `starlet_cache_thread_num` is `16`.

**Potential Exceptions**

Possible causes of replication failure include:
- Replication timeout due to network or disk I/O issues
- Server failure of the source or target CN

You can refer to [Observability](#observability) for trouble-shooting instructions. **Replication failure will not impact the transaction submission or data consistency.**
:::

#### `warmup_level`

- **Default**: NONE
- **Description**: Cache warmup level. Warmup indicates CN fetching the data or metadata of the tablet from the remote storage. When a new tablet is loaded to a CN node, the system will warm up the latest version of the tablet once according to the warmup level. The replica will not be visible until warmup succeeds. Valid values:
  - `NONE` (Default): The system will not warm up tablet metadata or data files.
  - `META`: The system will warm up the latest version of tablet metadata.
  - `INDEX`: The system will warm up the latest version of tablet metadata and the footer of the corresponding data files.
  - `ALL`: The system will warm up the latest version of tablet metadata and the corresponding data files.

:::note
**Related Configurations**

- The timeout duration for warmup is defined by the FE dynamic configuration item `lake_compute_replica_warmup_timeout_secs` (Unit: Seconds). The default value of `lake_compute_replica_warmup_timeout_secs` is `900`.
- The thread number for background warmup is defined by the CN dynamic configuration item `tablet_warmup_max_threads`. The default value of `tablet_warmup_max_threads` is `4`.

**Potential Exceptions**

The possible scenario of warmup failure can be that when the on-going warmup is interrupted by CN node failure, FE will consider the warmup a success and set the replica to visible after the warmup timeout.

You can refer to [Observability](#observability) for trouble-shooting instructions.
:::

## Best practices

### Combine multiple compute replicas with cache replication

Example:

```SQL
ALTER WAREHOUSE default_warehouse SET (
    "compute_replica" = "2", 
    "replication_type" = "SYNC"
);
```

The above example schedules two replicas for each tablet in the warehouse. When data generated by loading or Compaction operations is written into local disk cache of one CN, it will be synchronously replicated to the local disk cache of the other CN. Both replicas will be visible as soon as the transaction succeeds.

The combination of multiple compute replicas and cache replication will guarantee one replica is available when the other fails due to node crash or OOM, thus allowing of 100% cache hits.

In the production environment, it is recommended to enable two compute replicas and synchronous cache replication. More replicas indicates higher cache cost, while cache replication can barely affect the loading performance.

### Combine cache replication with cache warmup

Example:

```SQL
ALTER WAREHOUSE default_warehouse SET (
    "compute_replica" = "2", 
    "replication_type" = "SYNC", 
    "warmup_level" = "INDEX"
);
```

In addition to compute replicas, the above example also enables the warmup for latest metadata and footers for tablets when tablets are being re-distributed during node scaling operations within the warehouse.

The combination of cache replication and cache warmup alleviates possible significant changes in query latency caused by frequent scaling operations.

In the production environment, it is recommended to set the warmup level to `INDEX`. Setting it to `ALL` will bring significant I/O pressure to the node because all data files of the tablets will be loaded.

## Observability

### Cache replication

#### Monitoring metrics

- `file replicate total count`: The total number of files replicated.
- `file replicate fail count`: The number of replicated files that failed.
- `active file being replicated`: The number of files being received by the node for replication.
- `async file replication wait count`: The number of files in the queue waiting to be replicated, valid only for asynchronous replication.
- `async file replication timeout count`: The number of files timeout in the queue waiting for replication, valid only for asynchronous replication.
- `file replication send throughput`: The instantaneous size of data sent for the file replication.
- `file replication send latency (quantile)`: The p99 latency of the file replication send side.
- `file replication send latency (average)`: The average latency of the file replication send side.
- `file replication receive throughput`: The instantaneous size of data received for the file replication.
- `file replication receive latency (quantile)`: The p99 latency of the file replication receive side.
- `file replication receive throughput (average)`: The average latency of the file replication receive side.

#### Log

You can search keyword `cache_replication_token` in the CN log **CN.info** to identify the cause of replication failures.

### Cache warmup

#### Monitoring metrics

- `Warm Up Success Count`: The number of tablets that were successfully warmed up.
- `Warm Up Fail Count`: The number of tablets that failed to warm up.
- `Warm Up Current Count`: The number of tablets in the queue or being warmed up. The number of tablet being warmed up is controlled by the CN parameter `tablet_warmup_max_threads` (Default: `4`).
- `Warm Up Read Remote Per Minute`: The number of remote reads per minute during warmup.
`Warm Up Latency`: The average and p99 latency for the warmup of each tablet.

#### Log

You can search keyword `tablet_warmup_manager` in the CN log **CN.info** to identify the cause of warmup failures.
