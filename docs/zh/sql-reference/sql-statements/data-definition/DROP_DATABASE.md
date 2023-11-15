# DROP DATABASE

## 功能

该语句用于删除数据库（database）。

> **注意**
>
> 该操作需要有对应数据库的 DROP 权限。

## 语法

```sql
-- 2.0 之前版本
DROP DATABASE [IF EXISTS] [FORCE] db_name
-- 2.0 及以后版本
DROP DATABASE [IF EXISTS] db_name [FORCE]
```

说明：

1. 执行 DROP DATABASE 一段时间内（默认 1 天），可以通过 RECOVER 语句恢复被删除的数据库。详见 [RECOVER](../data-definition/RECOVER.md) 语句。

2. 如果执行 `DROP DATABASE FORCE`，则系统不会检查该数据库是否存在未完成的事务，数据库将直接被删除并且 **不能被恢复**，一般不建议执行此操作。

## 示例

1. 删除数据库 db_test。

    ```sql
    DROP DATABASE db_test;
    ```

## 相关参考

- [CREATE DATABASE](../data-definition/CREATE_DATABASE.md)
- [SHOW CREATE DATABASE](../data-manipulation/SHOW_CREATE_DATABASE.md)
- [USE](../data-definition/USE.md)
- [DESC](../Utility/DESCRIBE.md)
- [SHOW DATABASES](../data-manipulation/SHOW_DATABASES.md)
