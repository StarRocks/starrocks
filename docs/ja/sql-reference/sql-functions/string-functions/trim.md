---
displayed_sidebar: docs
---

# trim

`str` 引数の先頭と末尾から連続するスペースまたは指定された文字を削除します。指定された文字の削除は StarRocks 2.5.0 からサポートされています。

## Syntax

```Haskell
VARCHAR trim(VARCHAR str[, VARCHAR characters])
```

## Parameters

`str`: 必須、トリムする文字列で、VARCHAR 値に評価される必要があります。

`characters`: オプション、削除する文字で、VARCHAR 値である必要があります。このパラメータが指定されていない場合、デフォルトで文字列からスペースが削除されます。このパラメータが空文字列に設定されている場合、エラーが返されます。

## Return value

VARCHAR 値を返します。

## Examples

例 1: 文字列の先頭と末尾から 5 つのスペースを削除します。

```Plain Text
MySQL > SELECT trim("   ab c  ");
+-------------------+
| trim('   ab c  ') |
+-------------------+
| ab c              |
+-------------------+
1 row in set (0.00 sec)
```

例 2: 文字列の先頭と末尾から指定された文字を削除します。

```Plain Text
MySQL > SELECT trim("abcd", "ad");
+--------------------+
| trim('abcd', 'ad') |
+--------------------+
| bc                 |
+--------------------+

MySQL > SELECT trim("xxabcdxx", "x");
+-----------------------+
| trim('xxabcdxx', 'x') |
+-----------------------+
| abcd                  |
+-----------------------+
```

## References

- [ltrim](ltrim.md)
- [rtrim](rtrim.md)