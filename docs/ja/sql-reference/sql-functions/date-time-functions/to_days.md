---
displayed_sidebar: docs
---

# to_days

日付と 0000-01-01 の間の日数を返します。

`date` パラメータは DATE または DATETIME 型である必要があります。

## Syntax

```Haskell
INT TO_DAYS(DATETIME date)
```

## Examples

```Plain Text
MySQL > select to_days('2007-10-07');
+-----------------------+
| to_days('2007-10-07') |
+-----------------------+
|                733321 |
+-----------------------+
```

## keyword

TO_DAYS,TO,DAYS