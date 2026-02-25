---
keywords: ['xiugai'] 
displayed_sidebar: docs
---

# ALTER DATABASE

## 功能

该语句用于设置指定数据库的属性。

:::tip

该操作需要对应数据库的 ALTER 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

### 设置数据库数据量配额

单位为 B/K/KB/M/MB/G/GB/T/TB/P/PB。

```sql
ALTER DATABASE <db_name> SET DATA QUOTA <quota>
```

### 重命名数据库

```sql
ALTER DATABASE <db_name> RENAME <new_db_name>
```

### 设置数据库的副本数量配额

```sql
ALTER DATABASE <db_name> SET REPLICA QUOTA <quota>
```

### 设置数据库存储卷

```sql
ALTER DATABASE <db_name> SET ("storage_volume" = "<new_storage_volume_name>");
```

说明：

```plain text
重命名数据库后，如需要，请使用 REVOKE 和 GRANT 命令修改相应的用户权限。
数据库的默认数据量配额和默认副本数量配额均为 2^63 - 1。
如果未明确指定 "storage_volume" 属性，数据库的默认存储卷为创建数据库时的默认存储卷。
```

## 示例

1. 设置指定数据库数据量配额。

    ```SQL
    ALTER DATABASE example_db SET DATA QUOTA 10995116277760B;
    -- 上述单位为字节，等价于下面的语句
    ALTER DATABASE example_db SET DATA QUOTA 10T;
    ALTER DATABASE example_db SET DATA QUOTA 100G;
    ALTER DATABASE example_db SET DATA QUOTA 200M;
    ```

2. 将数据库 `example_db` 重命名为 `example_db2`。

    ```SQL
    ALTER DATABASE example_db RENAME example_db2;
    ```

3. 设定指定数据库副本数量配额。

    ```SQL
    ALTER DATABASE example_db SET REPLICA QUOTA 102400;
    ```

4. 设置数据库存储卷。

    ```SQL
    ALTER DATABASE example_db SET ("storage_volume" = "aws_s3_storage_volume");
    ```

## 相关参考

- [CREATE DATABASE](CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
