---
displayed_sidebar: docs
---

# current_version

StarRocks の現在のバージョンを返します。異なるクライアントとの互換性のために、2 つの構文が提供されています。

## Syntax

```Haskell
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