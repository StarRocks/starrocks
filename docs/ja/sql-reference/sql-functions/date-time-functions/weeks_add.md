---
displayed_sidebar: docs
---

# weeks_add

## 説明

指定された週数を日付に追加します。

## 構文

```Haskell
DATETIME weeks_add(DATETIME|DATE expr1, INT expr2);
```

## パラメータ

- `expr1`: 元の日付。DATETIME または DATE 型でなければなりません。

- `expr2`: 追加する週数。`INT` 型でなければなりません。

## 戻り値

DATETIME 値を返します。

日付が存在しない場合は `NULL` を返します。

## 例

```Plain
select weeks_add('2022-12-20',2);
+----------------------------+
| weeks_add('2022-12-20', 2) |
+----------------------------+
|        2023-01-03 00:00:00 |
+----------------------------+
```