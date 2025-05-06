---
displayed_sidebar: docs
---

# to_days

## 説明

日付と 0000-01-01 の間の日数を返します。

`date` パラメータは DATE または DATETIME 型でなければなりません。

## 構文

```Haskell
INT TO_DAYS(DATETIME date)
```

## 例

```Plain Text
MySQL > select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```

## キーワード

TO_DAYS, TO, DAYS