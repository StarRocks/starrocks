---
displayed_sidebar: docs
---

# array_repeat

## 功能

将一个给定的元素重复指定的次数，返回一个数组。

## 语法

```Haskell
array_repeat(element, count)
```

## 参数说明

* `element`：要重复的元素，类型为StarRocks支持的所有数据类型。
* `count`: 重复的次数,类型为 INT。

## 返回值说明

返回值的数据类型为element的ARRAY类型。

## 注意事项

- 当count小于1时返回空数组。
- 当element参数为 NULL 时，结果返回由count个NULL组成的数组。
- 当count参数为 NULL 时，结果返回 NULL。

## 示例

**示例一**

```plain text
mysql> select array_repeat(1,5) as res;
+-------------+
| res         |
+-------------+
| [1,1,1,1,1] |
+-------------+
```

**示例二**

```plain text
mysql> select  array_repeat([1,2],3) as res;
+-------------------+
| res               |
+-------------------+
| [[1,2],[1,2],[1,2]] |
+-------------------+
```

**示例三**

```plain text
mysql> select array_repeat(1,-1) as res;
+------+
| res  |
+------+
| []   |
+------+
```

**示例四**

```plain text
mysql> select  array_repeat(null,3) as res;
+------+
| res  |
+------+
| NULL |
+------+
```

**示例五**

```Plain
mysql> CREATE TABLE IF NOT EXISTS test (COLA INT, COLB INT) PROPERTIES ("replication_num"="1");
mysql> INTO test (COLA, COLB) VALUES (1, 3), (NULL, 3), (2, NULL);
mysql> select array_repeat(COLA,COLB) from test;
+--------------------------+
| array_repeat(COLA, COLB) |
+--------------------------+
| [1,1,1]                  |
| [null,null,null]         |
| NULL                     |
+--------------------------+
```