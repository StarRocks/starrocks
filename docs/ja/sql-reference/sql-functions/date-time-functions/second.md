---
displayed_sidebar: docs
---

# second

## 説明

指定された日付の秒の部分を返します。返される値は 0 から 59 の範囲です。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
INT SECOND(DATETIME date)
```

## 例

```Plain Text
MySQL > select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## キーワード

SECOND