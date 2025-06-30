---
sidebar_position: 40
---

# Primary Key tables

The Primary Key table can support real-time data updates while ensuring efficient query performance. However, the primary key is not a free lunch. If used improperly, it can lead to unnecessary resource waste.

## Choosing the Primary Index

The primary index is the most critical component in a Primary Key table.

By default, we enable persistent indexing `(enable_persistent_index=true)`.

:::tip
We do not recommend using in-memory indexing `(enable_persistent_index=false)`, as it can lead to significant memory resource waste.
:::

If you are using a shared-data (elastic) StarRocks cluster, we recommend opting for the cloud-native persistent index `(persistent_index_type=CLOUD_NATIVE)`. Unlike the disk-based persistent index `(persistent_index_type=LOCAL)`, it stores the complete index data on remote object storage, with local disks serving only as a cache. Compared to the disk-based persistent index, its advantages include:

1. No dependency on local disk capacity.
2. No need to rebuild indexes after replica rescheduling.

## Choosing the Primary key

The primary key usually does not help accelerate queries. You can specify a column different from the primary key as the sort key using the `ORDER BY` clause to speed up queries. Therefore, when selecting a primary key, you only need to consider the uniqueness during data import and update processes.

The larger the primary key, the more memory, I/O, and other resources it consumes. Therefore, it is generally recommended to avoid selecting too many or overly large columns as the primary key. The default maximum size for the primary key is 128 bytes, controlled by the `primary_key_limit_size` parameter in `be.conf`. 

You can increase `primary_key_limit_size` to select a larger primary key, but be aware that this will result in higher resource consumption.

### Formula for storage space cost

How much storage and memory space will a persistent index occupy?

```sql
(key size + 8 bytes) * row count * 50% 
```

:::note
50% is the estimated compression efficiency, the actual compression effect depends on the data itself.
:::

### Formula for memory cost

```sql
min(l0_max_mem_usage * tablet cnt, update_memory_limit_percent * BE process memory);
```

## Memory usage

If you are sensitive to memory usage and want to reduce memory consumption during the import process of a PK table, you can achieve this through the following configuration:

```sh
# be.conf

l0_max_mem_usage = (some value which smaller than 104857600, default is 104857600)
skip_pk_preload = true
transaction_apply_worker_count = (some value smaller than cpu core number, default is cpu core number)
```

Reducing `l0_max_mem_usage` may increase I/O pressure, while decreasing `transaction_apply_worker_count` could slow down data ingestion.

## Tradeoff between resource and data freshness

Compared to tables in other models, primary key tables require additional operations for primary key index lookups and delete vector generation during data import, updates, and deletions, which introduces extra resource overhead.


## Compaction
If you want to get better data freshness, that means you will introduce high frequency writes. Then you will need more compaction resource to handle these writes:

```sh
// shared-data
be.conf
compact_threads = 4

// shared-nothing
update_compaction_num_threads_per_disk = 1
update_compaction_per_tablet_min_interval_seconds = 120
```

You can increase compact_threads and `update_compaction_num_threads_per_disk`, or decease `update_compaction_per_tablet_min_interval_seconds` to introduce more compaction resource to handle high frequency writes.

