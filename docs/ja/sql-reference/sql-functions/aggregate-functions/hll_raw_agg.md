---
displayed_sidebar: docs
---

# hll_raw_agg

この関数は、HLL フィールドを集計するための集計関数です。HLL 値を返します。

## Syntax

```Haskell
hll_raw_agg(hll)
```

## Parameters

`hll`: 他のカラムによって生成されるか、ロードされたデータに基づく HLL カラム。

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