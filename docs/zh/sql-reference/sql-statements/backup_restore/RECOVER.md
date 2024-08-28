---
displayed_sidebar: docs
keywords: ['beifen']
---

# RECOVER

## 功能

恢复之前通过 DROP 操作删除的 database、table 或者 partition。DROP 操作后只有在指定的时间内才能通过 RECOVER 恢复数据，超过这个时间无法恢复。该时间由 FE 动态参数 `catalog_trash_expire_second` 控制，默认 1 天。

通过 [TRUNCATE TABLE](../table_bucket_part_index/TRUNCATE_TABLE.md) 命令删除的数据无法恢复。

> **注意**
>
> 只有拥有 default_catalog 的 CREATE DATABASE 权限才可以恢复数据库；同时需要拥有对应数据库的 CREATE TABLE 和对应表的 DROP 权限。

## 语法

### 恢复 database

```sql
RECOVER DATABASE <db_name>
```

### 恢复 table

```sql
RECOVER TABLE [db_name.]table_name;
```

### 恢复 partition

```sql
RECOVER PARTITION partition_name FROM [db_name.]table_name;
```

说明：

1. 该操作仅能恢复之前一段时间内删除的元信息，默认为 1 天。（可通过 `fe.conf` 中 `catalog_trash_expire_second` 参数配置。）

2. 如果删除元信息后新建了同名同类型的元信息，则之前删除的元信息不能被恢复。

## 示例

1. 恢复名为 `example_db` 的 database。

    ```sql
    RECOVER DATABASE example_db;
    ```

2. 恢复名为 `example_tbl` 的 table。

    ```sql
    RECOVER TABLE example_db.example_tbl;
    ```

3. 恢复表 `example_tbl` 中名为 `p1` 的 partition。

    ```sql
    RECOVER PARTITION p1 FROM example_tbl;
    ```
