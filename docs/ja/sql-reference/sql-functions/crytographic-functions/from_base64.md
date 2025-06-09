---
displayed_sidebar: docs
---

# from_base64

## Description

Base64 でエンコードされた文字列をデコードします。この関数は [to_base64](to_base64.md) の逆関数です。

## Syntax

```Haskell
from_base64(str);
```

## Parameters

`str`: デコードする文字列。VARCHAR 型である必要があります。

## Return value

VARCHAR 型の値を返します。入力が NULL または無効な Base64 文字列の場合、NULL が返されます。入力が空の場合、エラーが返されます。

この関数は 1 つの文字列のみを受け付けます。複数の入力文字列はエラーを引き起こします。

## Examples

```Plain Text
mysql> select from_base64("starrocks");
+--------------------------+
| from_base64('starrocks') |
+--------------------------+
| ²֫®$                       |
+--------------------------+
1 row in set (0.00 sec)

mysql> select from_base64('c3RhcnJvY2tz');
+-----------------------------+
| from_base64('c3RhcnJvY2tz') |
+-----------------------------+
| starrocks                   |
+-----------------------------+
```
