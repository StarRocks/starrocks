---
displayed_sidebar: docs
---

# 恢复数据

本文介绍如何恢复 StarRocks 中被删除的数据。

StarRocks 支持对误删除的数据库、表、分区进行数据恢复，在删除表或数据库之后，StarRocks 不会立刻对数据进行物理删除，而是将其保留在 Trash 一段时间（默认为 1 天）。管理员可以对误删除的数据进行恢复。

> 注意
>
> * 恢复操作仅能恢复一段时间内删除的元信息。默认为 1 天。您可通过配置 **fe.conf** 文件中的 `catalog_trash_expire_second` 参数修改。
> * 如果元信息被删除后，系统新建了同名同类型的元信息，则之前删除的元信息无法被恢复。

## 恢复数据库

通过以下命令恢复数据库。

```sql
RECOVER DATABASE db_name;
```

以下示例恢复名为 `example_db` 的数据库。

```sql
RECOVER DATABASE example_db;
```

## 恢复表

通过以下命令恢复表。

```sql
RECOVER TABLE [db_name.]table_name;
```

以下示例恢复 `example_db` 数据库中名为 `example_tbl` 的表。

```sql
RECOVER TABLE example_db.example_tbl;
```

## 恢复分区

通过以下命令恢复分区。

```sql
RECOVER PARTITION partition_name FROM [db_name.]table_name;
```

以下示例恢复 `example_db` 数据库的 `example_tbl` 表中名为 `p1` 的分区。

```sql
RECOVER PARTITION p1 FROM example_db.example_tbl;
```
