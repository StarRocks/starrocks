---
displayed_sidebar: docs
---

# ltrim

`str` 引数の先頭（左側）からスペースまたは指定された文字を削除します。指定された文字の削除は StarRocks 2.5.0 からサポートされています。

## Syntax

```Haskell
VARCHAR ltrim(VARCHAR str[, VARCHAR characters])
```

## Parameters

`str`: 必須、トリムする文字列で、VARCHAR 値に評価される必要があります。

`characters`: 任意、削除する文字で、VARCHAR 値である必要があります。このパラメータが指定されていない場合、デフォルトでスペースが文字列から削除されます。このパラメータが空の文字列に設定されている場合、エラーが返されます。

## Return value

VARCHAR 値を返します。

## Examples

例 1: 文字列の先頭からスペースを削除します。

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

例 2: 文字列の先頭から指定された文字を削除します。

```Plain Text
MySQL > SELECT ltrim("xxabcdxx", "x");
+------------------------+
| ltrim('xxabcdxx', 'x') |
+------------------------+
| abcdxx                 |
+------------------------+
```

## References

- [trim](trim.md)
- [rtrim](rtrim.md)

## keyword

LTRIM