---
displayed_sidebar: docs
---

# 数据恢复

恢复误删除的数据库/表/分区。在 `drop table` 或 `drop database` 后，StarRocks 不会立即物理删除数据，而是将其在回收站中保留一段时间（默认为 1 天）。管理员可以使用 `RECOVER` 命令恢复误删除的数据。

## 相关命令

语法:

```sql
-- 1) 恢复数据库
RECOVER DATABASE db_name;
-- 2) 恢复表
RECOVER TABLE [db_name.]table_name;
-- 3) 恢复分区
RECOVER PARTITION partition_name FROM [db_name.]table_name;
```

## 注意事项

1. 此操作只能恢复已删除的元信息。默认时间为 1 天，可以通过 `fe.conf` 中的 `catalog_trash_expire_second` 参数进行配置。
2. 如果元信息删除后，又创建了同名同类型的新元信息，则无法恢复之前删除的元信息。

## 示例

1. 恢复名为 `example_db` 的数据库

   ```sql
   RECOVER DATABASE example_db;
   ```

2. 恢复名为 `example_tbl` 的表

   ```sql
   RECOVER TABLE example_db.example_tbl;
   ```

3. 恢复表 `example_tbl` 中名为 `p1` 的分区

   ```sql
   RECOVER PARTITION p1 FROM example_tbl;
   ```
