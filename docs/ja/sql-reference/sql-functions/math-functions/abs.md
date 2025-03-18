---
displayed_sidebar: docs
---

# abs

数値 `x` の絶対値を返します。入力値が NULL の場合、NULL が返されます。

## Syntax

```Haskell
ABS(x);
```

## Parameters

`x`: 数値または式。

サポートされているデータ型: DOUBLE, FLOAT, LARGEINT, BIGINT, INT, SMALLINT, TINYINT, DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128.

## Return value

戻り値のデータ型は `x` の型と同じです。

## Examples

```Plain Text
mysql> select abs(-1);
+---------+
| abs(-1) |
+---------+
|       1 |
+---------+
1 row in set (0.00 sec)
```

## Keywords

abs, absolute