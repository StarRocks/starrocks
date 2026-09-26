---
displayed_sidebar: docs
description: "指定した解像度のH3セルの平均面積を平方メートル単位で返します。"
---

# h3HexAreaM2

指定した解像度のH3セルの平均面積を平方メートル単位で返します。

## Syntax

```Haskell
DOUBLE h3HexAreaM2(INT resolution)
```

## パラメータ

- `resolution`: H3解像度レベル（0〜15）。サポートされるデータ型: INT。

## 戻り値

指定した解像度での平均セル面積（平方メートル）をDOUBLEで返します。引数がNULL、または有効範囲外の場合はNULLを返します。

## 例

```sql
SELECT h3HexAreaM2(13);
+------------------+
| h3HexAreaM2(13)  |
+------------------+
| 43.9             |
+------------------+
```

## キーワード

H3HEXAREAM2,H3,SPATIAL
