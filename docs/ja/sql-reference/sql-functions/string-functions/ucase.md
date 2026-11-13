---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列を大文字に変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ucase

この関数は文字列を大文字に変換します。関数 upper と同様です。

## 構文

```plaintext
VARCHAR ucase(VARCHAR str)
```

## 例

```Plain Text
mysql> SELECT ucase("AbC123");
+-----------------+
|ucase('AbC123')  |
+-----------------+
|ABC123           |
+-----------------+
```

## キーワード

UCASE