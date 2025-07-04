---
displayed_sidebar: docs
---

# lookup_string

`lookup_string(table_name, lookup_key, return_column)`

此函数从主键表中查找值并在优化器中评估。

## 参数

`table_name`: 要查找的表名。必须是主键表 (VARCHAR)。
`lookup_key`: 要查找的键。必须是字符串类型 (VARCHAR)。
`return_column`: 要返回的列名 (VARCHAR)。

## 返回值

返回包含查找值的 VARCHAR 字符串。如果未找到则返回 `NULL`。

## 示例

示例1: 返回 `t2` 表中主键值等于 `1` 的 `event_day` 列值:
```
mysql> select lookup_string('t2', '1', 'event_day');
+---------------------------------------+
| lookup_string('t2', '1', 'event_day') |
+---------------------------------------+
| 2020-01-14                            |
+---------------------------------------+
1 row in set (0.02 sec)

``` 