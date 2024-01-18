---
displayed_sidebar: "Chinese"
---

# hll_union_agg

## 功能

该函数将多个 HLL 类型数据合并成一个 HLL。

HLL 是基于 HyperLogLog 算法的工程实现，用于保存 HyperLogLog 计算过程的中间结果。

它只能作为表的 value 列类型，通过聚合来不断的减少数据量，以此来实现加快查询的目的。

基于它得到的是一个估算结果，误差大概在 1% 左右。hll 列是通过其它列或者导入数据里面的数据生成的。

导入的时候通过 [hll_hash](../aggregate-functions/hll_hash.md) 函数来指定数据中哪一列用于生成 hll 列，它常用于替代 count distinct，通过结合 rollup 在业务上用于快速计算 uv 等。

## 语法

```Haskell
HLL_UNION_AGG(hll)
```

## 参数说明

`hll`: 通过其它列或者导入数据里面的数据生成的hhl列。

## 返回值说明

返回值为数值类型。

## 示例

```plain text
MySQL > select HLL_UNION_AGG(uv_set)
from test_uv;;
+-------------------------+
| HLL_UNION_AGG(`uv_set`) |
+-------------------------+
| 17721                   |
+-------------------------+
```
