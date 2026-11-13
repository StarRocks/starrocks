---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "2つの数値式のビット単位のANDを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bitand

2 つの数値式のビット単位の AND を返します。

## Syntax

```plaintext
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