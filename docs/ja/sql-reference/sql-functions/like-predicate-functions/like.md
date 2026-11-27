---
displayed_sidebar: docs
description: "与えられた式が指定されたパターンに対してあいまいに一致するかどうかをチェックします。"
products: [oss, enterprise, cloud, anywhere]
---

# like

指定されたパターンに対して、与えられた式があいまいに一致するかどうかをチェックします。一致する場合は 1 を返します。それ以外の場合は 0 を返します。入力パラメータのいずれかが NULL の場合は、NULL を返します。

LIKE は通常、パーセント記号 (%) やアンダースコア (_) などの文字と一緒に使用されます。`%` は 0、1、または複数の文字に一致します。`_` は任意の単一の文字に一致します。

## 構文

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## パラメータ

- `expr`: 文字列式。サポートされているデータ型は VARCHAR です。

- `pattern`: 一致させるパターン。サポートされているデータ型は VARCHAR です。

## エスケープ文字

リテラルの `%` または `_` に一致させるには、その前にエスケープ文字を置きます。デフォルトのエスケープ文字はバックスラッシュで、文字列リテラルでは `\\` と記述します。

LIKE を演算子として使用する場合、ESCAPE 句でパターンに別のエスケープ文字を指定できます。

```SQL
expr [NOT] LIKE pattern ESCAPE 'escape_char'
```

`escape_char` は 1 文字です。パターンにエスケープ文字がない場合は空の文字列を指定します。ESCAPE 句を使用する場合、`pattern` は定数式でなければなりません。

```SQL
-- c1 が "50%" で終わる行を返します。
SELECT * FROM t1 WHERE c1 LIKE '%50^%' ESCAPE '^';
```

## 戻り値

BOOLEAN 値を返します。

## 例

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