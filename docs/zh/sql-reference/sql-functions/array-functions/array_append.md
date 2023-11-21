---
displayed_sidebar: "Chinese"
---

# array_append

## 功能

在数组末尾添加一个新的元素。返回 ARRAY 类型的值。

## 语法

```Haskell
array_append(any_array, any_element)
```

## 示例

```plain text
mysql> select array_append([1, 2], 3);
+------------------------+
| array_append([1,2], 3) |
+------------------------+
| [1,2,3]                |
+------------------------+
1 row in set (0.00 sec)

```

可以向数组中添加 NULL。

```plain text
mysql> select array_append([1, 2], NULL);
+---------------------------+
| array_append([1,2], NULL) |
+---------------------------+
| [1,2,NULL]                |
+---------------------------+
1 row in set (0.01 sec)

```
