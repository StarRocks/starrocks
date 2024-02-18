---
displayed_sidebar: "Chinese"
---

# CREATE DATABASE

## 功能

该语句用于创建数据库 (database)。

:::tip

该操作需要对应 Catalog 下的 CREATE DATABASE 权限。请参考 [GRANT](../account-management/GRANT.md) 为用户赋权。

:::

## 语法

```sql
CREATE DATABASE [IF NOT EXISTS] db_name
```

`db_name`：数据库名称。有关数据库的命名要求，参见[系统限制](../../../reference/System_limit.md)。

## 示例

1. 新建数据库 db_test。

    ```sql
    CREATE DATABASE db_test;
    ```

## 相关参考

- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md)
- [DESC](../Utility/DESCRIBE.md)
- [DROP DATABASE](../data-definition/DROP_DATABASE.md)
