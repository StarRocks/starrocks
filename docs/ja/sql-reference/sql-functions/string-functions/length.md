---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列のバイト長を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# length

この関数は文字列の長さをバイト単位で返します。

## 構文

```plaintext
INT length(VARCHAR str)
```

## 例

```Plain Text
MySQL > select length("abc");
+---------------+
| length('abc') |
+---------------+
|             3 |
+---------------+
```

## キーワード

LENGTH