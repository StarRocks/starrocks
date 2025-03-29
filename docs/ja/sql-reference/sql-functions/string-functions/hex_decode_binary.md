---
displayed_sidebar: docs
---

# hex_decode_binary

16 進数でエンコードされた文字列をバイナリにデコードします。

この関数は v3.0 からサポートされています。

## Syntax

```Haskell
hex_decode_binary(str);
```

## Parameters

`str`: 変換する文字列。サポートされているデータ型は VARCHAR です。

以下のいずれかの状況が発生した場合、空のバイナリが返されます:

- 文字列の長さが 0 または文字列内の文字数が奇数である。
- 文字列に `[0-9]`、`[a-z]`、`[A-Z]` 以外の文字が含まれている。

## Return value

VARBINARY 型の値を返します。

## Examples

```Plain Text
mysql> select hex(hex_decode_binary(hex("Hello StarRocks")));
+------------------------------------------------+
| hex(hex_decode_binary(hex('Hello StarRocks'))) |
+------------------------------------------------+
| 48656C6C6F2053746172526F636B73                 |
+------------------------------------------------+

mysql> select hex_decode_binary(NULL);
+--------------------------------------------------+
| hex_decode_binary(NULL)                          |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
```

## Keywords

HEX_DECODE_BINARY