---
displayed_sidebar: docs
---

# least

1 つ以上のパラメータのリストから最小の値を返します。

一般的に、戻り値は入力と同じデータ型を持ちます。

比較ルールは [greatest](greatest.md) 関数と同じです。

## Syntax

```Haskell
LEAST(expr1,...);
```

## Parameters

`expr1`: 比較する式。以下のデータ型をサポートします:

- SMALLINT

- TINYINT

- INT

- BIGINT

- LARGEINT

- FLOAT

- DOUBLE

- DECIMALV2

- DECIMAL32

- DECIMAL64

- DECIMAL128

- DATETIME

- VARCHAR

## Examples

Example 1: 単一の入力に対して最小の値を返します。

```Plain
select least(3);
+----------+
| least(3) |
+----------+
|        3 |
+----------+
1 row in set (0.00 sec)
```

Example 2: 値のリストから最小の値を返します。

```Plain
select least(3,4,5,5,6);
+----------------------+
| least(3, 4, 5, 5, 6) |
+----------------------+
|                    3 |
+----------------------+
1 row in set (0.01 sec)
```

Example 3: 1 つのパラメータが DOUBLE 型で、DOUBLE 値が返されます。

```Plain
select least(4,4.5,5.5);
+--------------------+
| least(4, 4.5, 5.5) |
+--------------------+
|                4.0 |
+--------------------+
```

Example 4: 入力パラメータは数値と文字列の混合ですが、文字列は数値に変換可能です。パラメータは数値として比較されます。

```Plain
select least(7,'5');
+---------------+
| least(7, '5') |
+---------------+
| 5             |
+---------------+
1 row in set (0.01 sec)
```

Example 5: 入力パラメータは数値と文字列の混合ですが、文字列は数値に変換できません。パラメータは文字列として比較されます。文字列 `'1'` は `'at'` より小さいです。

```Plain
select least(1,'at');
+----------------+
| least(1, 'at') |
+----------------+
| 1              |
+----------------+
```

Example 6: 入力パラメータは文字です。

```Plain
mysql> select least('A','B','Z');
+----------------------+
| least('A', 'B', 'Z') |
+----------------------+
| A                    |
+----------------------+
1 row in set (0.00 sec)
```

## Keywords

LEAST, least