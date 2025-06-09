---
displayed_sidebar: docs
---

# weekofyear

## 説明

指定された日付の年内の週番号を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
INT WEEKOFYEAR(DATETIME date)
```

## 例

```Plain Text
MySQL > select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## キーワード

WEEKOFYEAR