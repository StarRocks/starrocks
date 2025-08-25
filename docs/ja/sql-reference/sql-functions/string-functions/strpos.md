---
displayed_sidebar: docs
---

## strpos

文字列内の N 番目のサブストリングの位置を返します。instance が負の数の場合、検索は文字列の末尾から開始されます。位置は 1 から始まります。見つからない場合は 0 が返されます。

## Syntax

```Haskell
INT strpos(VARCHAR str, VARCHAR substr [, INT instance])
```

## Examples

```SQL
SELECT strpos('hello world', 'world');
+-----------------------------+
| strpos('hello world', 'world') |
+-----------------------------+
|                           7 |
+-----------------------------+

SELECT strpos('Hello World', 'world');
+-----------------------------+
| strpos('Hello World', 'world') |
+-----------------------------+
|                           0 |
+-----------------------------+

SELECT strpos('hello world hello', 'hello', 2);
+--------------------------------------+
| strpos('hello world hello', 'hello', 2) |
+--------------------------------------+
|                                   13 |
+--------------------------------------+

SELECT strpos('StarRocks', 'Spark');
+----------------------------+
| strpos('StarRocks', 'Spark') |
+----------------------------+
|                          0 |
+----------------------------+

SELECT strpos(NULL, 'test');
+--------------------+
| strpos(NULL, 'test') |
+--------------------+
|               NULL |
+--------------------+
```

## キーワード

STRPOS, STRING, POSITION, SUBSTRING