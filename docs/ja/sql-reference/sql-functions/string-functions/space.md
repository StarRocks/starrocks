---
displayed_sidebar: docs
---

# space

指定された数のスペースを含む文字列を返します。

## Syntax

```Haskell
space(x);
```

## Parameters

`x`: 返すスペースの数。サポートされているデータ型は INT です。

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select space(6);
+----------+
| space(6) |
+----------+
|          |
+----------+
1 row in set (0.00 sec)
```

## Keywords

SPACE