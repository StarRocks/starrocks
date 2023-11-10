---
displayed_sidebar: "Chinese"
---

# array_remove

## 功能

从数组中移除指定元素。

## 语法

```Haskell
array_remove(any_array, any_element)
```

## 参数说明

* any_array: 目标数组
* any_element: 需要被移除的元素

## 返回值说明

返回移除元素后的数组。

## 示例

```plain text
mysql> select array_remove([1,2,3,null,3], 3);
+---------------------------------+
| array_remove([1,2,3,NULL,3], 3) |
+---------------------------------+
| [1,2,null]                      |
+---------------------------------+
1 row in set (0.01 sec)
```
