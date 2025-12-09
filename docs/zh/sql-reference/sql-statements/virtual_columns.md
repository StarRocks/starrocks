---
displayed_sidebar: docs
---

# 虚拟列

StarRocks 支持虚拟列（Virtual Column），这是一种只读的元数据列，自动可用于查询但不会存储在表结构中。虚拟列提供关于数据存储结构的元数据信息，可用于数据分布分析和故障排查。

目前，StarRocks 支持以下虚拟列：

- `_tablet_id_`：返回每行数据所属的 tablet ID（BIGINT 类型），用于标识该行数据属于哪个 tablet。

## 特性

- **默认隐藏**：虚拟列不会出现在 `SELECT *`、`DESCRIBE` 或 `SHOW CREATE TABLE` 语句中。
- **只读**：虚拟列是自动计算的，无法修改或插入。
- **查询支持**：虚拟列可以在 SELECT、WHERE、GROUP BY、ORDER BY、JOIN 和聚合函数中使用。

## 语法

在查询中使用虚拟列时，需要在 SELECT 子句中显式指定：

```SQL
SELECT _tablet_id_, <其他列> FROM <表名>;
```

## 常见使用场景

### 1. 数据分布分析

分析数据在 tablet 间的分布情况，识别数据倾斜：

```SQL
SELECT _tablet_id_,
       COUNT(*) AS row_count,
       SUM(k2) AS total_value
FROM test_table
GROUP BY _tablet_id_
ORDER BY row_count DESC;
```

### 2. Tablet 级别统计

获取每个 tablet 的统计信息，了解数据分布：

```SQL
SELECT _tablet_id_,
       COUNT(*) AS rows,
       MIN(k1) AS min_key,
       MAX(k1) AS max_key,
       AVG(k2) AS avg_value
FROM test_table
GROUP BY _tablet_id_;
```

### 3. 故障排查

识别包含特定数据的 tablet，用于故障排查：

```SQL
-- 查找包含特定值的 tablet
SELECT _tablet_id_, k1, k2
FROM test_table
WHERE k1 = 100;
```

## 限制

- 虚拟列**不会包含**在 `SELECT *` 语句中。必须在 SELECT 子句中显式指定。
- 虚拟列**不会出现**在 `DESCRIBE` 或 `SHOW CREATE TABLE` 语句中。
- 虚拟列**无法修改**或直接插入。
- 虚拟列是**只读**的，在查询时计算。

## 注意事项

- `_tablet_id_` 的值是一个 BIGINT 类型，唯一标识包含该行的 tablet。
- 虚拟列在查询执行时计算，不占用存储空间。
- 虚拟列可以与所有标准 SQL 操作一起使用，包括 WHERE、GROUP BY、ORDER BY、JOIN 和聚合函数。
