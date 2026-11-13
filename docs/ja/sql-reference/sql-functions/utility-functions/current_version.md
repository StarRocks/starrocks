---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "StarRocksの現在のバージョンを返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# current_version

StarRocks の現在のバージョンを返します。異なるクライアントとの互換性のために、2 つの構文が提供されています。

## Syntax

```plaintext
current_version();

@@version_comment;
```

## Parameters

なし

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select current_version();
+-------------------+
| current_version() |
+-------------------+
| 2.1.2 0782ad7     |
+-------------------+
1 row in set (0.00 sec)

mysql> select @@version_comment;
+-------------------------+
| @@version_comment       |
+-------------------------+
| StarRocks version 2.1.2 |
+-------------------------+
1 row in set (0.01 sec)
```