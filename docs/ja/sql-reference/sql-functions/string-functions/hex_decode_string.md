---
displayed_sidebar: docs
---

# hex_decode_string

この関数は [hex()](hex.md) の逆の操作を行います。

入力文字列の各16進数のペアを数値として解釈し、その数値が表すバイトに変換します。戻り値はバイナリ文字列です。

この関数は v3.0 からサポートされています。

## 構文

```Haskell
hex_decode_string(str);
```

## パラメータ

`str`: 変換する文字列。サポートされているデータ型は VARCHAR です。以下のいずれかの状況が発生した場合、空の文字列が返されます。

- 文字列の長さが 0 または文字列内の文字数が奇数である。
- 文字列に `[0-9]`、`[a-z]`、および `[A-Z]` 以外の文字が含まれている。

## 戻り値

VARCHAR 型の値を返します。

## 例

```Plain Text
mysql> select hex_decode_string(hex("Hello StarRocks"));
+-------------------------------------------+
| hex_decode_string(hex('Hello StarRocks')) |
+-------------------------------------------+
| Hello StarRocks                           |
+-------------------------------------------+
```

## キーワード

HEX_DECODE_STRING