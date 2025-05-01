---
displayed_sidebar: docs
---

# rtrim

## 説明

`str` 引数の末尾（右側）から空白または指定された文字を削除します。指定された文字の削除は StarRocks 2.5.0 からサポートされています。

## 構文

```Haskell
VARCHAR rtrim(VARCHAR str[, VARCHAR characters]);
```

## パラメータ

`str`: 必須、トリムする文字列で、VARCHAR 値に評価される必要があります。

`characters`: 任意、削除する文字で、VARCHAR 値である必要があります。このパラメータが指定されていない場合、デフォルトで文字列から空白が削除されます。このパラメータが空文字列に設定されている場合、エラーが返されます。

## 戻り値

VARCHAR 値を返します。

## 例

例 1: 文字列から末尾の3つの空白を削除します。

```Plain Text
mysql> SELECT rtrim('   ab d   ');
+---------------------+
| rtrim('   ab d   ') |
+---------------------+
|    ab d             |
+---------------------+
1 row in set (0.00 sec)
```

例 2: 文字列の末尾から指定された文字を削除します。

```Plain Text
MySQL > SELECT rtrim("xxabcdxx", "x");
+------------------------+
| rtrim('xxabcdxx', 'x') |
+------------------------+
| xxabcd                 |
+------------------------+
```

## 参照

- [trim](trim.md)
- [ltrim](ltrim.md)