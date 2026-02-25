---
displayed_sidebar: docs
---

# hour

指定された日付の時間を返します。返される値は 0 から 23 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## Syntax

```Haskell
INT HOUR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select hour('2018-12-31 23:59:59');
+-----------------------------+
| hour('2018-12-31 23:59:59') |
+-----------------------------+
|                          23 |
+-----------------------------+
```

## keyword

HOUR