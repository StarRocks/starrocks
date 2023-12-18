---
displayed_sidebar: "Chinese"
---

# last_query_id

## 功能

返回最近一次执行的查询的 ID。

## 语法

```Haskell
VARCHAR last_query_id();
```

## 参数说明

无。

## 返回值说明

返回 VARCHAR 类型的值。

## 示例

```Plain Text
mysql> select last_query_id();
+--------------------------------------+
| last_query_id()                      |
+--------------------------------------+
| 7c1d8d68-bbec-11ec-af65-00163e1e238f |
+--------------------------------------+
1 row in set (0.00 sec)
```
