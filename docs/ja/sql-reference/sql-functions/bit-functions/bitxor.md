---
displayed_sidebar: docs
---

# bitxor

2 つの数値式のビットごとの XOR を返します。

## Syntax

```Haskell
BITXOR(x,y);
```

## Parameters

- `x`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

- `y`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

> `x` と `y` はデータ型が一致している必要があります。

## Return value

戻り値は `x` と同じ型です。いずれかの値が NULL の場合、結果は NULL です。

## Examples

```Plain Text
mysql> select bitxor(3,0);
+--------------+
| bitxor(3, 0) |
+--------------+
|            3 |
+--------------+
1 row in set (0.00 sec)
```