---
displayed_sidebar: docs
description: "指定した解像度のH3セルの平均辺長をメートル単位で返します。"
---

# h3EdgeLengthM

指定した解像度のH3セルの平均辺長をメートル単位で返します。

## Syntax

```Haskell
DOUBLE h3EdgeLengthM(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での平均辺長（メートル）をDOUBLEで返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT h3EdgeLengthM(15);
+--------------------+
| h3EdgeLengthM(15)  |
+--------------------+
| 0.509713273        |
+--------------------+
```

## キーワード

H3EDGELENGTHM,H3,SPATIAL
