# DROP DATABASE

## description

该语句用于删除数据库（database）

语法：

```sql
-- 2.0以前版本
DROP DATABASE [IF EXISTS] [FORCE] db_name;
--2.0及以后版本
DROP DATABASE [IF EXISTS] db_name [FORCE];
```

说明：

1. 执行 DROP DATABASE 一段时间内，可以通过 RECOVER 语句恢复被删除的数据库。详见 RECOVER 语句

2. 如果执行 DROP DATABASE FORCE，则系统不会检查该数据库是否存在未完成的事务，数据库将直接被删除并且不能被恢复，一般不建议执行此操作

## example

1. 删除数据库 db_test

    ```sql
    DROP DATABASE db_test;
    ```

## keyword

DROP,DATABASE
