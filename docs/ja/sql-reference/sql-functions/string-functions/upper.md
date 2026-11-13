---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列を大文字に変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# upper

文字列を大文字に変換します。

## 構文

```plaintext
upper(str)
```

## パラメータ

- `str`: 変換する文字列。`str` が文字列型でない場合、最初に暗黙的なキャストを試みます。

## 戻り値

大文字の文字列を返します。

## 例

```plaintext
MySQL [test]> select C_String, upper(C_String) from ex_iceberg_tbl;
+-------------------+-------------------+
| C_String          | upper(C_String)   |
+-------------------+-------------------+
| Hello, StarRocks! | HELLO, STARROCKS! |
| Hello, World!     | HELLO, WORLD!     |
+-------------------+-------------------+
```