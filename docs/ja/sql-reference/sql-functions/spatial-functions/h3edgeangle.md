---
displayed_sidebar: docs
description: "指定した解像度のH3セルの平均辺長を度単位で返します。"
---

# h3EdgeAngle

指定した解像度のH3セルの平均辺長を度単位で返します。

## Syntax

```Haskell
DOUBLE h3EdgeAngle(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での平均辺長（度）をDOUBLEで返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT h3EdgeAngle(10);
+------------------------+
| h3EdgeAngle(10)        |
+------------------------+
| 0.0005927224846720883  |
+------------------------+
```

## キーワード

H3EDGEANGLE,H3,SPATIAL
