---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列内のすべての文字を小文字に変換します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# lower

引数内のすべての文字列を小文字に変換します。

## 構文

```plaintext
INT lower(VARCHAR str)
```

## 例

```Plain Text
MySQL > SELECT lower("AbC123");
+-----------------+
| lower('AbC123') |
+-----------------+
| abc123          |
+-----------------+
```

## キーワード

LOWER