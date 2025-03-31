---
displayed_sidebar: docs
---

# conv

数値 `x` をある数値基数システムから別のシステムに変換し、その結果を文字列値として返します。

## Syntax

```Haskell
CONV(x,y,z);
```

## Parameters

- `x`: 変換する数値。VARCHAR または BIGINT がサポートされています。
- `y`: 元の基数。TINYINT がサポートされています。
- `z`: 変換先の基数。TINYINT がサポートされています。

## Return value

VARCHAR データ型の値を返します。

## Examples

数値 8 を数値基数システム 2 から数値基数システム 10 に変換します。

```Plain
mysql> select conv(8,10,2);
+----------------+
| conv(8, 10, 2) |
+----------------+
| 1000           |
+----------------+
1 row in set (0.00 sec)
```