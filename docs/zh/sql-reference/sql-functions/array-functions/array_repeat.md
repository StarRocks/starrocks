---
displayed_sidebar: "Chinese"
---

# array_repeat

## 功能

将一个给定的元素重复指定的次数，返回一个数组。

## 语法

```Haskell
array_repeat(element, count)
```

## 参数说明

* `element`：要重复的元素，类型为 BOOLEAN、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、DECIMALV2、VARCHAR、DATETIME、DATE、JSON 或它们的ARRAY类型。
* `count`: 重复的次数,类型为 INT。

## 返回值说明

返回值的数据类型为element的ARRAY类型。

## 注意事项

- 当count小于1时返回空数组。
- 当任意参数为 NULL 时，结果返回 NULL。

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
