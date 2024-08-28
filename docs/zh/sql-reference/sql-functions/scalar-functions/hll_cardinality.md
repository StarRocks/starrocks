---
displayed_sidebar: docs
---

# hll_cardinality

## 功能

用于计算 HLL 类型值的基数。

## 语法

```Haskell
HLL_CARDINALITY(hll)
```

## 参数说明

`hll`: 通过其它列或者导入数据里面的数据生成的 HLL 列。

## 返回值说明

返回 BIGINT 类型的值。

## 示例

```plain text
MySQL > select HLL_CARDINALITY(uv_set) from test_uv;
+---------------------------+
| hll_cardinality(`uv_set`) |
+---------------------------+
|                         3 |
+---------------------------+
```
