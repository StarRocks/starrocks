---
displayed_sidebar: docs
---

# weeks_add

日付に週数を追加した値を返します。

## 構文

```Haskell
DATETIME weeks_add(DATETIME expr1, INT expr2);
```

## パラメータ

- `expr1`: 元の日付。`DATETIME` 型である必要があります。

- `expr2`: 週数。`INT` 型である必要があります。

## 戻り値

`DATETIME` を返します。

日付が存在しない場合は `NULL` が返されます。

## 例

```Plain
select weeks_add('2022-12-20',2);
+----------------------------+
| weeks_add('2022-12-20', 2) |
+----------------------------+
|        2023-01-03 00:00:00 |
+----------------------------+
```