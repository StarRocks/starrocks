---
displayed_sidebar: docs
---

# current_version

## 功能

获取当前 StarRocks 的版本，当前为了兼容不同的客户端，提供两种语法。

## 语法

```Haskell
current_version();

@@version_comment;
```

## 参数说明

无

## 返回值说明

返回值的数据类型为 VARCHAR。

## 示例

```Plain Text
mysql> select current_version();
+-------------------+
| current_version() |
+-------------------+
| 2.1.2 0782ad7     |
+-------------------+
1 row in set (0.00 sec)

mysql> select @@version_comment;
+-------------------------+
| @@version_comment       |
+-------------------------+
| StarRocks version 2.1.2 |
+-------------------------+
1 row in set (0.01 sec)
```
