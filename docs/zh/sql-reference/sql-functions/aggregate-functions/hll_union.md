---
displayed_sidebar: "Chinese"
---


# hll_union

## 功能

返回一组 HLL 值的并集。

## 语法

```Haskell
hll_union(hll)
```

## 参数说明

`hll`: 通过其它列或者导入数据里面的数据生成的 hll 列。

## 返回值说明

返回值为 hll 类型。

## 示例

```Plain
mysql> select k1, hll_cardinality(hll_union(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_union(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```
