---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "文字列内の部分文字列の最初の出現位置を1から始まる位置で返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# instr

この関数は、`str` が `substr` に最初に現れた位置を返します（1から数え始め、文字数で測定します）。`str` が `substr` に見つからない場合、この関数は 0 を返します。

## Syntax

```plaintext
INT instr(VARCHAR str, VARCHAR substr)
```

## Examples

```Plain Text
MySQL > select instr("abc", "b");
+-------------------+
| instr('abc', 'b') |
+-------------------+
|                 2 |
+-------------------+

MySQL > select instr("abc", "d");
+-------------------+
| instr('abc', 'd') |
+-------------------+
|                 0 |
+-------------------+
```

## keyword

INSTR