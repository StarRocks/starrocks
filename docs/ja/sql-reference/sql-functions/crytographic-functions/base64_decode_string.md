---
displayed_sidebar: docs
---

# base64_decode_string

この関数は [from_base64](from_base64.md) と同じです。Base64 エンコードされた文字列をデコードします。[to_base64](to_base64.md) の逆です。

この関数は v3.0 からサポートされています。

## 構文

```Haskell
base64_decode_string(str);
```

## パラメータ

`str`: デコードする文字列。VARCHAR 型でなければなりません。

## 戻り値

VARCHAR 型の値を返します。入力が NULL または無効な Base64 文字列の場合、NULL が返されます。入力が空の場合、エラーが返されます。

この関数は 1 つの文字列のみを受け付けます。複数の入力文字列はエラーを引き起こします。

## 例

```Plain Text

mysql> select base64_decode_string(to_base64("Hello StarRocks"));
+----------------------------------------------------+
| base64_decode_string(to_base64('Hello StarRocks')) |
+----------------------------------------------------+
| Hello StarRocks                                    |
+----------------------------------------------------+
```