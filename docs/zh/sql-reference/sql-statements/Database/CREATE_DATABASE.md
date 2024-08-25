---
displayed_sidebar: docs
---

# CREATE DATABASE

## 功能

该语句用于创建数据库 (database)。

:::tip

该操作需要对应 Catalog 下的 CREATE DATABASE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
CREATE DATABASE [IF NOT EXISTS] <db_name>
[PROPERTIES ("key"="value", ...)]
```

## 参数说明

`db_name`：数据库名称。有关数据库的命名要求，参见[系统限制](../../System_limit.md)。

**PROPERTIES（选填）**

`storage_volume`: 存算分离集群中，用于存储当前数据库中数据的存储卷。如未指定，则使用默认存储卷。

## 示例

1. 新建数据库 `db_test`。

   ```sql
   CREATE DATABASE db_test;
   ```

2. 基于存储卷 `s3_storage_volume` 新建云原生数据 `cloud_db`。

   ```sql
   CREATE DATABASE cloud_db
   PROPERTIES ("storage_volume"="s3_storage_volume");
   ```

## 相关参考

- [SHOW CREATE DATABASE](SHOW_CREATE_DATABASE.md)
- [USE](USE.md)
- [SHOW DATABASES](SHOW_DATABASES.md)
- [DESC](../table_bucket_part_index/DESCRIBE.md)
- [DROP DATABASE](DROP_DATABASE.md)
