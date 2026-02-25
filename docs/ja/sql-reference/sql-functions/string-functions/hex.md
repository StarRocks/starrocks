---
displayed_sidebar: docs
---

# hex

`x` が数値の場合、この関数はその値の16進数表現を返します。

`x` が文字列の場合、この関数は文字列の各文字を2つの16進数に変換した文字列の16進数表現を返します。

## Syntax

```Haskell
HEX(x);
```

## Parameters

`x`: 変換する文字列または数値。サポートされているデータ型は BIGINT、VARCHAR、および VARBINARY (v3.0以降) です。

## Return value

VARCHAR 型の値を返します。

## Examples

```Plain Text
mysql> select hex(3);
+--------+
| hex(3) |
+--------+
| 3      |
+--------+
1 row in set (0.00 sec)

mysql> select hex('3');
+----------+
| hex('3') |
+----------+
| 33       |
+----------+
1 row in set (0.00 sec)

mysql> select hex('apple');
+--------------+
| hex('apple') |
+--------------+
| 6170706C65   |
+--------------+

-- 入力がバイナリ値の場合。

mysql> select hex(x'abab');
+-------------+
| hex('ABAB') |
+-------------+
| ABAB        |
+-------------+
1 row in set (0.01 sec)
```

## Keywords

HEX