---
displayed_sidebar: docs
---

# from_days

## 説明

0000-01-01 からの日付を返します。

## 構文

```Haskell
DATE FROM_DAYS(INT N)
```

## 例

```Plain Text
MySQL > select from_days(730669);
+-------------------+
| from_days(730669) |
+-------------------+
| 2000-07-03        |
+-------------------+
```

## キーワード

FROM_DAYS, FROM, DAYS