---
displayed_sidebar: docs
---

# version

MySQL データベースの現在のバージョンを返します。

StarRocks のバージョンを照会するには、[current_version](current_version.md) を使用できます。

## Syntax

```Haskell
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

[current_version](../utility-functions/current_version.md)