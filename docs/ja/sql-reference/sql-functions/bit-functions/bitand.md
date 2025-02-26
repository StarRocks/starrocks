---
displayed_sidebar: docs
---

# bitand

2 つの数値式のビット単位の AND を返します。

## Syntax

```Haskell
BITAND(x,y);
```

## Parameters

- `x`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

- `y`: この式は、TINYINT、SMALLINT、INT、BIGINT、LARGEINT のいずれかのデータ型に評価される必要があります。

> `x` と `y` はデータ型が一致している必要があります。

## Return value

戻り値は `x` と同じ型です。いずれかの値が NULL の場合、結果は NULL になります。

## Examples

```Plain Text
mysql> select bitand(3,0);
+--------------+
| bitand(3, 0) |
+--------------+
|            0 |
+--------------+
1 row in set (0.01 sec)
```