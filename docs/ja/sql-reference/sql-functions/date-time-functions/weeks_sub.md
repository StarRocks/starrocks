---
displayed_sidebar: docs
---

# weeks_sub

指定された週数を datetime または date 値から減算します。

## Syntax

```Haskell
DATETIME weeks_sub(DATETIME expr1, INT expr2);
```

## Parameters

- `expr1`: 元の日付です。`DATETIME` 型である必要があります。

- `expr2`: 週の数です。`INT` 型である必要があります。

## Return value

`DATETIME` 値を返します。

日付が存在しない場合は `NULL` が返されます。

## Examples

```Plain
select weeks_sub('2022-12-22',2);
+----------------------------+
| weeks_sub('2022-12-22', 2) |
+----------------------------+
|        2022-12-08 00:00:00 |
+----------------------------+
```