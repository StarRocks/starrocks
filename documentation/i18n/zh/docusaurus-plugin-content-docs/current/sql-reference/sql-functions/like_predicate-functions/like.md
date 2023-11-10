# like

## 功能

判断字符串 `expr` 是否**模糊匹配**给定的模式 `pattern`，匹配成功返回 1，否则返回 0。LIKE 通常与 `%`、`_` 结合使用，`%` 表示 0 个、一个或多个字符，`_` 表示单个字符。

## 语法

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## 参数说明

`expr`: 目标字符串，支持的数据类型为 VARCHAR。

`pattern`: 字符串需匹配的模式，支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BOOLEAN。

## 示例

```Plain Text

mysql> select like("star","star");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+


mysql> select like("starrocks","star%");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+
1 row in set (0.00 sec)
```
