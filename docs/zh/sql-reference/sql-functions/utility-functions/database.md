# database

## 功能

查询当前会话所在的数据库。如果未选定数据库，返回空值。

## 语法

```Haskell
database()
```

## 参数说明

该函数不需要传入参数。

## 返回值说明

返回当前会话所在的数据库名称。

## 示例

```SQL
-- 选择一个目标数据库。
use db_test

-- 返回当前数据库名称。
select database();
+------------+
| DATABASE() |
+------------+
| db_test    |
+------------+
```

## 相关 SQL

[USE](../../sql-statements/data-definition/USE.md)：切换到指定数据库。
