---
displayed_sidebar: docs
---

# to_base64

文字列を Base64 エンコードされた文字列に変換します。この関数は [from_base64](from_base64.md) の逆関数です。

## 構文

```Haskell
to_base64(str);
```

## パラメータ

`str`: エンコードする文字列。VARCHAR 型でなければなりません。

## 戻り値

VARCHAR 型の値を返します。入力が NULL の場合、NULL が返されます。入力が空の場合、エラーが返されます。

この関数は 1 つの文字列のみを受け付けます。複数の入力文字列はエラーを引き起こします。

## 例

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