---
displayed_sidebar: docs
sidebar_label: "Memory and Spill"
sidebar_position: 4
description: "Session variables that control query memory limits and spilling to disk or remote storage."
---

# System Variables - Memory and Spill

For how to view and set variables, see the [System variables overview](../System_variable.md).

import Restricted from '../../_assets/commonMarkdown/_restricted.mdx'

### disable_spill_to_local_disk

* **Description**: When set to `true` for the session, FE will instruct BE to disable spilling to local disk and instead rely on remote storage spill (if remote spill is configured). This flag is only meaningful when `enable_spill` = `true`, `enable_spill_to_remote_storage` = `true`, and a valid `spill_storage_volume` is provided and found by FE. The value is serialized into TSpillToRemoteStorageOptions (sent to BE) as `disable_spill_to_local_disk`. If remote spill is not configured or the named storage volume cannot be resolved, this setting has no effect. Use with caution: disabling local-disk spill can increase network I/O and latency and requires reliable, performant remote storage.
* **Scope**: Session
* **Default**: false
* **Data Type**: boolean
* **Introduced in**: v3.3.0, v3.4.0, v3.5.0

### enable_connector_sink_spill

<Restricted />

* **Description**: Enables spilling to disk when connector sink operations exceed memory limits.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_spill

* **Description**: Whether to enable intermediate result spilling. Default: `false`. If it is set to `true`, StarRocks spills the intermediate results to disk to reduce the memory usage when processing aggregate, sort, or join operators in queries.
* **Default**: false
* **Introduced in**: v3.0

### enable_spill_buffer_read

<Restricted />

* **Description**: Enables buffering when reading spilled data to disk during query execution.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### enable_spill_to_remote_storage

* **Description**: Whether to enable intermediate result spilling to object storage. If it is set to `true`, StarRocks spills the intermediate results to the storage volume specified in `spill_storage_volume` after the capacity limit of the local disk is reached. For more information, see [Spill to object storage](../../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage).
* **Default**: false
* **Introduced in**: v3.3.0

### exec_mem_limit

<Restricted />

* **Description**: Limits the maximum memory allocated for executing a single query on each backend node.
* **Scope**: Session
* **Default**: `DEFAULT_EXEC_MEM_LIMIT`
* **Data type**: `long`
* **Mutable**: Yes

### max_spill_read_buffer_bytes_per_driver

<Restricted />

* **Description**: Specifies the maximum buffer size in bytes per driver for reading spilled data during query execution.
* **Scope**: Session
* **Default**: `1024 * 1024 * 16`
* **Data type**: `long`
* **Mutable**: Yes

### query_mem_limit

* **Description**: Used to set the memory limit of a query on each BE node. The default value is 0, which means no limit for it. This item takes effect only after Pipeline Engine is enabled. When the `Memory Exceed Limit` error happens, you could try to increase this variable. Setting it to `0` indicates no limit is imposed.
* **Default**: 0
* **Unit**: Byte

### spill_enable_compaction

<Restricted />

* **Description**: Enables compression of spilled data to disk to reduce storage space usage.
* **Scope**: Session
* **Default**: `true`
* **Data type**: `boolean`
* **Mutable**: Yes

### spill_enable_direct_io

* **Description**: Enables direct I/O operations when spilling intermediate query results to disk.
* **Scope**: Session
* **Default**: `false`
* **Data type**: `boolean`
* **Mutable**: Yes

### spill_encode_level

* **Scope**: Session
* **Description**: Controls the encoding/compression behaviour applied to operator spill files. The integer is a bit-flag level whose meanings mirror `transmission_encode_level`:
  * bit 1 (value `1`) — enable adaptive encoding;
  * bit 2 (value `2`) — encode integer-like columns with streamvbyte;
  * bit 4 (value `4`) — compress binary/string columns with LZ4.
  Example semantics from the related `transmission_encode_level` comment: `7` enables adaptive encoding for numbers and strings; `6` forces encoding of numbers and strings. Changing this value adjusts CPU vs. disk I/O trade-offs for spills (higher encoding levels increase CPU work but reduce spill size / I/O).
  Implemented as the session variable annotated `SPILL_ENCODE_LEVEL` in `SessionVariable.java` (getter `getSpillEncodeLevel()`), and documented adjacent to other spill tunables such as `spill_mem_table_size`.
* **Default**: `7`
* **Data Type**: int
* **Introduced in**: v3.2.0

### spill_mem_limit_threshold

<Restricted />

* **Description**: Specifies the memory utilization threshold that triggers data spilling to disk during query execution.
* **Scope**: Session
* **Default**: `0.8`
* **Data type**: `double`
* **Mutable**: Yes

### spill_mem_table_num

<Restricted />

* **Description**: Specifies the number of in-memory tables to maintain before spilling data to disk.
* **Scope**: Session
* **Default**: `2`
* **Data type**: `int`
* **Mutable**: Yes

### spill_mem_table_size

<Restricted />

* **Description**: These parameters are experimental. They may be removed in the future
* **Scope**: Session
* **Default**: `1024 * 1024 * 100`
* **Data type**: `int`
* **Mutable**: Yes

### spill_mode (3.0 and later)

The execution mode of intermediate result spilling. Valid values:

* `auto`: Spilling is automatically triggered when the memory usage threshold is reached.
* `force`: StarRocks forcibly executes spilling for all relevant operators, regardless of memory usage.

This variable takes effect only when the variable `enable_spill` is set to `true`.

### spill_operator_max_bytes

<Restricted />

* **Description**: Limits the maximum amount of memory an operator can use before spilling data to disk.
* **Scope**: Session
* **Default**: `1024L * 1024 * 1000`
* **Data type**: `long`
* **Mutable**: Yes

### spill_operator_min_bytes

<Restricted />

* **Description**: Specifies the minimum amount of data an operator must accumulate before spilling to disk.
* **Scope**: Session
* **Default**: `1024L * 1024 * 50`
* **Data type**: `long`
* **Mutable**: Yes

### spill_partitionwise_agg

* **Description**: Session-level flag that enables partition-wise aggregation behavior when spill is used for aggregation operators. When `spill_partitionwise_agg` is true (and `enable_spill` is enabled), the execution engine will partition spilled aggregation data and perform per-partition spill/merge processing. The flag is propagated to execution via `TSpillOptions.setSpill_partitionwise_agg`. Related session settings that affect its behavior are `spill_partitionwise_agg_partition_num` (number of partitions created) and `spill_partitionwise_agg_skew_elimination` (skew handling). This option reduces peak memory usage for large-group aggregations by splitting work across partitions during spill, but may increase I/O and merge overhead.
* **Scope**: Session
* **Default**: `false`
* **Data Type**: boolean
* **Introduced in**: v3.5.2

### spill_rand_ratio

<Restricted />

* **Description**: Controls the probability ratio for randomly triggering spill operations in test scenarios.
* **Scope**: Session
* **Default**: `0.1`
* **Data type**: `double`
* **Mutable**: Yes

### spill_revocable_max_bytes

* **Scope**: Session
* **Description**: Experimental per-session threshold (in bytes) for operator revocable memory. If an operator's revocable memory exceeds this value, the operator will initiate spilling to disk "as soon as possible" to free revocable memory. Use this to tune aggressive spilling for memory‑intensive operators; the value is interpreted in bytes.
* **Default**: `0`
* **Data Type**: long
* **Introduced in**: v3.2.0

### spill_storage_volume

* **Description**: The storage volume with which you want to store the intermediate results of queries that triggered spilling. For more information, see [Spill to object storage](../../administration/management/resource_management/spill_to_disk.md#preview-spill-intermediate-result-to-object-storage).
* **Default**: Empty string
* **Introduced in**: v3.3.0

### spillable_operator_mask

<Restricted />

* **Description**: this is used to control which operators can spill, only meaningful when enable_spill=true it uses a bit to identify whether the spill of each operator is in effect, 0 means no, 1 means yes at present, only the lowest 4 bits are meaningful, corresponding to the four operators HASH_JOIN, AGG, AGG_DISTINCT and SORT respectively (see TSpillableOperatorType in InternalService.thrift) e.g. if spillable_operator_mask & 1 != 0, hash join operator can spill if spillable_operator_mask & 2 != 0, agg operator can spill if spillable_operator_mask & 4 != 0, agg distinct operator can spill if spillable_operator_mask & 8 != 0, sort operator can spill if spillable_operator_mask & 16 != 0, nest loop join operator can spill ... default value is -1, means all operators can spill
* **Scope**: Session
* **Default**: `-1`
* **Data type**: `long`
* **Mutable**: Yes

