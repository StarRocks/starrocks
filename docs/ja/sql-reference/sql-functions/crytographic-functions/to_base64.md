---
displayed_sidebar: docs
---

# to_base64

## Description

文字列をBase64エンコードされた文字列に変換します。この関数は [from_base64](from_base64.md) の逆関数です。

## Syntax

```Haskell
to_base64(str);
```

## Parameters

`str`: エンコードする文字列。VARCHAR型である必要があります。

## Return value

VARCHAR型の値を返します。入力がNULLの場合、NULLが返されます。入力が空の場合、エラーが返されます。

この関数は1つの文字列のみを受け付けます。複数の入力文字列はエラーを引き起こします。

## Examples

```Plain Text
mysql> select to_base64("starrocks");
+------------------------+
| to_base64('starrocks') |
+------------------------+
| c3RhcnJvY2tz           |
+------------------------+
1 row in set (0.00 sec)

mysql> select to_base64(123);
+----------------+
| to_base64(123) |
+----------------+
| MTIz           |
+----------------+
```