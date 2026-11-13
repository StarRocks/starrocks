---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "2つの数値式のビットごとのXORを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitxor

2 つの数値式のビットごとの XOR を返します。

## Syntax

```plaintext
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