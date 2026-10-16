---
displayed_sidebar: docs
sidebar_position: 70
description: "How to use StarRocks virtual columns such as _tablet_id_, _segment_id_, and _row_id_ to inspect where each row of a native table is physically stored."
keywords: ['虚拟列', 'virtual column', 'tablet_id', 'row_id', 'segment_id', 'rowset_id']
---

# 虚拟列

虚拟列（Virtual column）是 StarRocks 在内表上提供的一组只读元数据列，用于描述**每一行数据的物理存储位置**，即该行来自哪个 Tablet、哪个 Rowset、哪个 Segment 以及 Segment 内的第几行。

虚拟列在查询时由 BE（或 CN）节点计算生成，不会落盘存储，不占用存储空间，也不属于表结构。因此虚拟列：

- 不会出现在 `SELECT *` 的结果中；
- 不会出现在 `DESCRIBE <table>` 和 `SHOW CREATE TABLE <table>` 的结果中；
- 必须显式指定列名才能查询。

虚拟列主要用于诊断场景，例如分析数据在各个 Tablet 上的分布情况，或定位某一行数据的物理位置。

## 虚拟列列表

| 列名               | 数据类型 | 说明                                                              |
| ----------------- | -------- | ----------------------------------------------------------------- |
| `_tablet_id_`     | BIGINT   | 该行数据所在 Tablet 的 ID。                                          |
| `_rowset_id_`     | STRING   | 该行数据所在 Rowset 的 ID。                                          |
| `_segment_id_`    | BIGINT   | 该行数据所在 Segment 的 ID。                                         |
| `_rss_id_`        | INT      | 该行数据在 Tablet 内的 Rowset-Segment ID，由 Rowset ID 和 Segment ID 计算得到。 |
| `_dynamic_rssid_` | INT      | 该行数据的动态 Rowset-Segment ID。                                    |
| `_row_id_`        | BIGINT   | 该行数据在所属 Segment 内的行号，从 0 开始。                            |
| `_source_id_`     | INT      | 读取该行数据的 BE 或 CN 节点的 ID。                                    |

虚拟列名不区分大小写，`_tablet_id_` 与 `_TABLET_ID_` 指向同一个虚拟列。

`_tablet_id_`、`_rowset_id_`（或 `_rss_id_`）、`_segment_id_` 和 `_row_id_` 组合起来，可以标识一行数据在被读取时的物理位置。

## 使用方式

虚拟列适用于 Internal Catalog 中的内表（明细表、聚合表、更新表和主键表）以及物化视图。External Catalog 中的表不提供虚拟列。

在查询中，凡是可以使用普通列的位置都可以引用虚拟列，包括 `SELECT` 列表、`WHERE`、`GROUP BY`、`ORDER BY`、聚合函数和 JOIN。当查询涉及多张表时，需要通过表名或别名限定虚拟列，例如 `t._tablet_id_`。

以下示例均基于该表：

```sql
CREATE TABLE olap_table (
    k1 INT,
    k2 INT,
    v1 STRING
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;

INSERT INTO olap_table VALUES
(1, 10, 'a'),
(2, 20, 'b'),
(3, 30, 'c'),
(4, 40, 'd'),
(5, 50, 'e');
```

### 查询虚拟列

```sql
SELECT _tablet_id_, k1, k2 FROM olap_table ORDER BY k1;
+------------+------+------+
| _tablet_id_| k1   | k2   |
+------------+------+------+
|    3619836 |    1 |   10 |
|    3619834 |    2 |   20 |
|    3619836 |    3 |   30 |
|    3619838 |    4 |   40 |
|    3619838 |    5 |   50 |
+------------+------+------+
```

虚拟列不会出现在 `SELECT *` 的结果中：

```sql
SELECT * FROM olap_table ORDER BY k1;
+------+------+------+
| k1   | k2   | v1   |
+------+------+------+
|    1 |   10 | a    |
|    2 |   20 | b    |
|    3 |   30 | c    |
|    4 |   40 | d    |
|    5 |   50 | e    |
+------+------+------+
```

### 分析数据在 Tablet 上的分布

按 `_tablet_id_` 分组，可以查看每个 Tablet 中的数据行数，从而快速判断是否因分桶键选择不当导致数据倾斜。

```sql
SELECT _tablet_id_, COUNT(*) AS row_count, SUM(k2) AS sum_k2
FROM olap_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
+------------+-----------+--------+
| _tablet_id_| row_count | sum_k2 |
+------------+-----------+--------+
|    3619836 |         2 |     40 |
|    3619838 |         2 |     90 |
|    3619834 |         1 |     20 |
+------------+-----------+--------+
```

统计实际存有数据的 Tablet 数量：

```sql
SELECT COUNT(DISTINCT _tablet_id_) FROM olap_table;
```

### 定位一行数据的物理位置

```sql
SELECT k1, _tablet_id_, _rowset_id_, _segment_id_, _row_id_
FROM olap_table
WHERE k1 = 1;
```

### 基于虚拟列过滤数据

```sql
SELECT k1, k2, _tablet_id_
FROM olap_table
WHERE _tablet_id_ = 3619836
ORDER BY k1;
```

:::note
虚拟列上的谓词在扫描数据的过程中计算，并不会减少查询需要读取的 Tablet 数量。
:::

## 使用说明

- 虚拟列描述的是物理存储信息，而非业务数据。当底层数据被重组时（例如 Compaction、新的导入、Schema Change 或副本迁移），虚拟列的值可能发生变化。请勿将其作为业务主键保存，也不要依赖它在多次查询之间关联数据行。
- 虚拟列仅支持查询，不能被导入、更新，也不能在 `CREATE TABLE` 中声明。
- StarRocks 不会为虚拟列收集列统计信息。
- 虚拟列的列名为系统保留名。默认情况下，您不能创建同名的表列，因为同名的表列会遮蔽虚拟列，导致两者在查询中都无法被引用。如果确实需要使用此类列名，请将 FE 动态参数 [`allow_system_reserved_names`](../administration/configuration/FE_parameters/shared_lake_other.md#allow_system_reserved_names) 设置为 `TRUE`。详见 [CREATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE.md)。

## 启用或禁用虚拟列

虚拟列由 FE 动态参数 [`enable_virtual_columns`](../administration/configuration/FE_parameters/user_query_loading.md#enable_virtual_columns) 控制，默认值为 `true`。

如需在集群中禁用虚拟列：

```sql
ADMIN SET FRONTEND CONFIG ("enable_virtual_columns" = "false");
```

禁用后，查询中引用虚拟列会报错，例如 `Column '_tablet_id_' cannot be resolved`。
