---
displayed_sidebar: docs
---

# inspect_all_pipes

`inspect_all_pipes()`

此函数返回当前数据库中所有管道的元数据。

## 参数

无。

## 返回值

返回包含所有管道元数据的 JSON 格式的 VARCHAR 字符串。

## 示例

示例 1: 获取当前所有管道

```
mysql> select inspect_all_pipes();
+---------------------+
| inspect_all_pipes() |
+---------------------+
| []                  |
+---------------------+
1 row in set (0.01 sec)
```

