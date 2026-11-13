---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "入力文字列の32ビットのmurmur3ハッシュ値を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# murmur_hash3_32

入力文字列の32ビットの murmur3 ハッシュ値を返します。

## Syntax

```plaintext
INT MURMUR_HASH3_32(VARCHAR input, ...)
```

## Examples

```Plain Text
MySQL > select murmur_hash3_32(null);
+-----------------------+
| murmur_hash3_32(NULL) |
+-----------------------+
|                  NULL |
+-----------------------+

MySQL > select murmur_hash3_32("hello");
+--------------------------+
| murmur_hash3_32('hello') |
+--------------------------+
|               1321743225 |
+--------------------------+

MySQL > select murmur_hash3_32("hello", "world");
+-----------------------------------+
| murmur_hash3_32('hello', 'world') |
+-----------------------------------+
|                         984713481 |
+-----------------------------------+
```

## keyword

MURMUR_HASH3_32,HASH