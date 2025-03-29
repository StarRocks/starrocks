---
displayed_sidebar: docs
---

# array_flatten

## 功能

将嵌套数组展平一层

## 语法

```Haskell
array_flatten(param)
```

## 参数说明

`param`：需要展平的嵌套数组，只支持嵌套数组，可以是多层嵌套数组，数组元素可以是StarRocks支持的所有数据类型。

## 返回值说明

返回值的数据类型为展平一层后的数组类型。

## 示例

**示例一：2层嵌套数组展平。**

```plain text
mysql> SELECT array_flatten([[1, 2], [1, 4]]) as res;
+-----------+
| res       |
+-----------+
| [1,2,1,4] |
+-----------+
```

**示例二：3层嵌套数组展平。**

```plain text
mysql> SELECT array_flatten([[[1],[2]], [[3],[4]]]) as res;
+-------------------+
| res               |
+-------------------+
| [[1],[2],[3],[4]] |
+-------------------+
```