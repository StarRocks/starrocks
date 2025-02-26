---
displayed_sidebar: docs
---

# second

指定された日付の秒の部分を返します。返される値は 0 から 59 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
INT SECOND(DATETIME date)
```

## Examples

```Plain Text
MySQL > select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

SECOND