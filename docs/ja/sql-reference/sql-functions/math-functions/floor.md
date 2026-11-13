---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "x 以下の最大の整数を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# floor, dfloor

`x` 以下の最大の整数を返します。

## Syntax

```SQL
FLOOR(x);
```

## Parameters

`x`: DOUBLE がサポートされています。

## Return value

BIGINT データ型の値を返します。

## Examples

```Plaintext
mysql> select floor(3.14);
+-------------+
| floor(3.14) |
+-------------+
|           3 |
+-------------+
1 row in set (0.01 sec)
```