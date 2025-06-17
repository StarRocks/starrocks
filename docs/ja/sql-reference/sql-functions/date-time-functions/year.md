---
displayed_sidebar: docs
---

# year

日付の年の部分を返し、1000 から 9999 の範囲の値を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
INT YEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select year('1987-01-01');
+-----------------------------+
| year('1987-01-01 00:00:00') |
+-----------------------------+
|                        1987 |
+-----------------------------+
```

## keyword

YEAR