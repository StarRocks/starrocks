---
displayed_sidebar: "Chinese"
---

# array_length

## 功能

返回数组中元素个数，返回值类型是 INT。如果参数是 NULL，返回值也是 NULL。

## 语法

```Haskell
array_length(any_array)
```

## 示例

```plain text
mysql> select array_length([1,2,3]);
+-----------------------+
| array_length([1,2,3]) |
+-----------------------+
|                     3 |
+-----------------------+
1 row in set (0.00 sec)

mysql> select array_length([[1,2], [3,4]]);
+-----------------------------+
| array_length([[1,2],[3,4]]) |
+-----------------------------+
|                           2 |
+-----------------------------+
1 row in set (0.01 sec)
```
