---
displayed_sidebar: docs
description: "指定した解像度のH3セルの平均辺長をキロメートル単位で返します。"
---

# h3EdgeLengthKm

指定した解像度のH3セルの平均辺長をキロメートル単位で返します。

## Syntax

```Haskell
DOUBLE h3EdgeLengthKm(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での平均辺長（キロメートル）をDOUBLEで返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT h3EdgeLengthKm(15);
+---------------------+
| h3EdgeLengthKm(15)  |
+---------------------+
| 0.000509713         |
+---------------------+
```

## キーワード

H3EDGELENGTHKM,H3,SPATIAL
