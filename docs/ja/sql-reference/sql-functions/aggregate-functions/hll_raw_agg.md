---
displayed_sidebar: docs
---

# hll_raw_agg

## Description

この関数は、HLL フィールドを集計するために使用される集計関数です。HLL 値を返します。

## Syntax

```Haskell
hll_raw_agg(hll)
```

## Parameters

`hll`: 他の列によって生成されるか、ロードされたデータに基づく HLL 列。

## Return value

HLL 型の値を返します。

## Examples

```Plain
mysql> select k1, hll_cardinality(hll_raw_agg(v1)) from tbl group by k1;
+------+----------------------------------+
| k1   | hll_cardinality(hll_raw_agg(`v1`)) |
+------+----------------------------------+
|    2 |                                4 |
|    1 |                                3 |
+------+----------------------------------+
```