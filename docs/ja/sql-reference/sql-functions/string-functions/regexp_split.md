---
displayed_sidebar: docs
---

# regexp_split

文字列 `str` を正規表現 `pattern` で分割し、`ARRAY<VARCHAR>` 型で最大 `max_split` 要素を返します。

## Syntax

```Haskell
regexp_split(str, pattern[, max_split])
```

## Parameters

`str`: 必須、分割する文字列で、`VARCHAR` 値に評価される必要があります。

`pattern`: 必須、分割に使用する正規表現パターンで、`VARCHAR` 値に評価される必要があります。

`max_split`: 任意、出力値の最大要素数で、`INT` 値に評価される必要があります。

## Return value

`ARRAY<VARCHAR>` 値を返します。

## Examples

```Plain Text
mysql> select regexp_split('StarRocks', '');
+---------------------------------------+
| regexp_split('StarRocks', '')         |
+---------------------------------------+
| ["S","t","a","r","R","o","c","k","s"] |
+---------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]');
+-----------------------------------+
| regexp_split('StarRocks', '[SR]') |
+-----------------------------------+
| ["","tar","ocks"]                 |
+-----------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 1);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["StarRocks"]                        |
+--------------------------------------+

mysql> select regexp_split('StarRocks', '[SR]', 2);
+--------------------------------------+
| regexp_split('StarRocks', '[SR]', 1) |
+--------------------------------------+
| ["","tarRocks"]                      |
+--------------------------------------+
```