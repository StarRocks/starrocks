# version

## 功能

返回当前 MySQL 数据库的版本。可以使用 `current_version` 函数查询 StarRocks 当前的版本。

## 语法

```Haskell
version();
```

## 参数说明

无。该函数不接受任何参数。

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

## 相关文档

[current_version](/sql-reference/sql-functions/utility-functions/current_version.md)
