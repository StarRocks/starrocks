---
displayed_sidebar: docs
---

# dayofyear

指定された日付の年の日数を返します。

`date` パラメータは DATE または DATETIME 型である必要があります。

## 構文

```Haskell
INT DAYOFYEAR(DATETIME|DATE date)
```

## 例

```Plain Text
MySQL > select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```

## キーワード

DAYOFYEAR