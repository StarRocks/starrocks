---
displayed_sidebar: docs
---

# like

## Description

指定されたパターンに対して、与えられた式があいまいに一致するかどうかをチェックします。一致する場合は 1 を返します。それ以外の場合は 0 を返します。入力パラメータのいずれかが NULL の場合、NULL を返します。

LIKE は通常、パーセント記号 (%) やアンダースコア (_) などの文字と一緒に使用されます。`%` は 0 個、1 個、または複数の文字に一致します。`_` は任意の単一の文字に一致します。

## Syntax

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## Parameters

- `expr`: 文字列式。サポートされているデータ型は VARCHAR です。

- `pattern`: 一致させるパターン。サポートされているデータ型は VARCHAR です。

## Return value

BOOLEAN 値を返します。

## Examples

```Plain Text
mysql> select like("star","star");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+

mysql> select like("starrocks","star%");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+

mysql> select like("starrocks","star_");
+----------------------------+
| like('starrocks', 'star_') |
+----------------------------+
|                          0 |
+----------------------------+
```