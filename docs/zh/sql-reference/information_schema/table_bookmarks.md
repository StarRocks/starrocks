---
displayed_sidebar: docs
description: 展示 OlapTable 当前生效的 Bookmark，包含清单、捕获的分区版本以及当前持有者。
---

# table_bookmark_summary / _partitions / _references

`information_schema.table_bookmark_summary`、`table_bookmark_partitions` 和
`table_bookmark_references` 用于展示 StarRocks 集群上当前生效的 Bookmark 状态。
Bookmark 是 OlapTable 分区状态的一份不可变记录，在某一时刻取得，用于锁定分区版本，
从而支持 Vacuum 和基于时间戳的查询。

可通过 `(DB_ID, TABLE_ID, BOOKMARK_ID)` 关联这三张表。

## table_bookmark_summary

**Bookmark 清单。** 每个活跃 Bookmark 对应一行，包含创建时间、分区/引用数，以及概览聚合（最近发生变化的 3 个分区，以及最早和最新的持有者）。当你想了解某张表或整个集群上存在哪些 Bookmark 时，首先查询此表。

| 字段 | 类型 | 描述 |
|------|------|------|
| DB_ID | BIGINT | 数据库的内部 ID。 |
| TABLE_ID | BIGINT | 表的内部 ID。 |
| BOOKMARK_ID | BIGINT | Bookmark ID（在表内唯一）。 |
| CREATE_TIME | DATETIME | Bookmark 创建时间。 |
| LOGICAL_PARTITION_COUNT | BIGINT | 该 Bookmark 捕获的逻辑分区数量。 |
| PHYSICAL_PARTITION_COUNT | BIGINT | 该 Bookmark 捕获的物理分区数量。 |
| REFERENCE_COUNT | BIGINT | 当前引用此 Bookmark 的持有者数量。 |
| LATEST_CHANGED_PHYSICAL_PARTITIONS | ARRAY<STRUCT<id BIGINT, version BIGINT, time DATETIME>> | 最多返回 3 个 `visible_version_time` 最新的物理分区，按时间倒序排列。时间相同时，按 `physical_partition_id` 从大到小排序。若 Bookmark 未捕获任何分区，返回空数组；分区不足 3 个时，返回的数组长度也相应少于 3。 |
| OLDEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME, ttl_ms BIGINT> | 当前 acquire 时间最早的持有者。时间相同时，按持有者 ID 字典序最小的优先。`ttl_ms` 为该持有者的引用级 TTL，单位为毫秒（`<= 0` 表示无 TTL）。 |
| NEWEST_REFERENCE | STRUCT<id VARCHAR, time DATETIME, ttl_ms BIGINT> | 当前 acquire 时间最新的持有者。并列时的处理规则同上。`ttl_ms` 为该持有者的引用级 TTL，单位为毫秒（`<= 0` 表示无 TTL）。 |

## table_bookmark_partitions

**分区粒度明细。** 每个 (bookmark, physical_partition) 对应一行，精确展示某个 Bookmark 锁定了哪些分区版本。在调试 Vacuum 时使用此表：在 `physical_partition_id` 上与 `partitions_meta` 关联，定位仍然持有比当前表更旧版本的 Bookmark。

| 字段 | 类型 | 描述 |
|------|------|------|
| DB_ID, TABLE_ID, BOOKMARK_ID | （同 summary） | 关联键。 |
| LOGICAL_PARTITION_ID | BIGINT | 逻辑分区 ID。 |
| PHYSICAL_PARTITION_ID | BIGINT | 物理分区 ID。 |
| VISIBLE_VERSION | BIGINT | Bookmark 中捕获的该分区的可见版本。 |
| VISIBLE_VERSION_TIME | DATETIME | 该可见版本变为可见的实际时间。 |
| BASE_MATERIALIZED_INDEX_META_ID | BIGINT | Bookmark 时刻的 Base Materialized Index Meta ID。在 Schema Change / Reshard 后会发生变化。 |
| BASE_MATERIALIZED_INDEX_ID | BIGINT | Bookmark 时刻的 Base Materialized Index ID。 |

## table_bookmark_references

**持有者粒度明细。** 每个 (bookmark, holder) 对应一行，展示当前是哪些持有者让该 Bookmark 保持存活，以及它们的获取时间。当需要查找某个物化视图或其他持有者所持有的 Bookmark 时，使用此表。

| 字段 | 类型 | 描述 |
|------|------|------|
| DB_ID, TABLE_ID, BOOKMARK_ID | （同 summary） | 关联键。 |
| HOLDER_ID | VARCHAR | 持有者标识。物化视图的编码形式为 `mv:<dbId>-<mvId>`。 |
| CREATE_TIME | DATETIME | 该持有者获取此 Bookmark 的时间。 |
| TTL_MS | BIGINT | 在 acquire 时设置的引用级生存时间（TTL），单位为毫秒。`<= 0` 表示无 TTL，后台清理任务自身不会让该引用过期。实际生存时间取此值与集群上限 `bookmark_reference_max_ttl_ms` 中较小的一个。 |

## 查询示例

### 清单：列出某张表的所有 Bookmark

```sql
SELECT * FROM information_schema.table_bookmark_summary
WHERE table_id = <table_id>;
```

### Vacuum 调试：查找被最老 Bookmark 锁定的分区

```sql
SELECT p.physical_partition_id, p.visible_version
FROM information_schema.table_bookmark_partitions p
JOIN information_schema.table_bookmark_summary s
  USING (db_id, table_id, bookmark_id)
WHERE s.table_id = <table_id>
ORDER BY s.create_time ASC
LIMIT 100;
```

### MV 持有者查询：查找某个 MV 持有的 Bookmark

```sql
SELECT * FROM information_schema.table_bookmark_references
WHERE holder_id = 'mv:1001-2003';
```

## 注意事项

- 行结果会按权限过滤：用户只能看到自己拥有 `SELECT` 权限的表上的 Bookmark。
