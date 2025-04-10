---
displayed_sidebar: docs
---

# DROP STATS

## 功能

删除 CBO 统计信息。

StarRocks 支持手动删除统计信息。手动删除统计信息时，会删除统计信息数据和统计信息元数据，并且会删除过期内存中的统计信息缓存。需要注意的是，如果当前存在自动采集任务，可能会重新采集之前已删除的统计信息。您可以使用`SHOW ANALYZE STATUS`查看统计信息采集历史记录。该语句从 2.4 版本开始支持。

您可以使用 DROP STATS 删除基础统计信息，使用 ANALYZE TABLE DROP HISTOGRAM 删除直方图统计信息。

### 删除基础统计信息

#### 语法

```SQL
DROP STATS <tbl_name>
```

### 删除直方图统计信息

#### 语法

```SQL
ANALYZE TABLE tbl_name DROP HISTOGRAM ON <col_name> [, <col_name>]
```

## 相关文档

想了解更多 CBO 统计信息采集的内容，参见[CBO 统计信息](../../../using_starrocks/Cost_based_optimizer.md)。
