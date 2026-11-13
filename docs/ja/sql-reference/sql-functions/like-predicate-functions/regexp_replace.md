---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "正規表現パターンに一致する文字列の部分を置換文字列に置き換えます。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# regexp_replace

この関数は、正規表現パターンに一致する `str` 内の文字列を `repl` で置き換えます。

## Syntax

```plaintext
VARCHAR regexp_replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

## 例

```Plain Text
MySQL > SELECT regexp_replace('a b c', " ", "-");
+-----------------------------------+
| regexp_replace('a b c', ' ', '-') |
+-----------------------------------+
| a-b-c                             |
+-----------------------------------+

MySQL > SELECT regexp_replace('a b c','(b)','<\\1>');
+----------------------------------------+
| regexp_replace('a b c', '(b)', '<\1>') |
+----------------------------------------+
| a <b> c                                |
+----------------------------------------+
```

## キーワード

REGEXP_REPLACE,REGEXP,REPLACE