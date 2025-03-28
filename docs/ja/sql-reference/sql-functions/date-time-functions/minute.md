---
displayed_sidebar: docs
---

# minute

指定された日付の分を返します。返り値は 0 から 59 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
INT MINUTE(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

MINUTE