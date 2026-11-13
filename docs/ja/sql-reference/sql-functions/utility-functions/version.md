---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "MySQLデータベースの現在のバージョンを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# version

MySQL データベースの現在のバージョンを返します。

StarRocks のバージョンを照会するには、[current_version](current_version.md) を使用できます。

## Syntax

```plaintext
VARCHAR version();
```

## Parameters

なし

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select version();
+-----------+
| version() |
+-----------+
| 5.1.0     |
+-----------+
1 row in set (0.00 sec)
```

## References

[current_version](./current_version.md)