---
displayed_sidebar: docs
---

# minute

## 説明

指定された日付の分を返します。返される値は0から59の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
INT MINUTE(DATETIME|DATE date)
```

## 例

```Plain Text
MySQL > select minute('2018-12-31 23:59:59');
+-----------------------------+
|minute('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## キーワード

MINUTE