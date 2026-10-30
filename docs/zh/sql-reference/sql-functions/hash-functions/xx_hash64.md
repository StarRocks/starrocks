---
displayed_sidebar: docs
description: "返回输入字符串的 64 位 XXH64 hash 值。"
---

# xx_hash64

返回输入字符串的 64 位 XXH64 hash 值。

## 语法

```Haskell
BIGINT XX_HASH64(VARCHAR input, ...)
```

## 示例

```Plain Text
MySQL > select xx_hash64(null);
+-----------------+
| xx_hash64(NULL) |
+-----------------+
|            NULL |
+-----------------+
```

```Plain Text
MySQL > select xx_hash64("hello");
+---------------------+
| xx_hash64('hello')  |
+---------------------+
| 2794345569481354659 |
+---------------------+
```

```Plain Text
MySQL > select xx_hash64("hello", "world");
+-----------------------------+
| xx_hash64('hello', 'world') |
+-----------------------------+
|         8004569595807101537 |
+-----------------------------+
```

## 相关函数

- [`xx_hash32`](./xx_hash32.md)
- [`xx_hash3_64`](./xx_hash3_64.md)

## keyword

XX_HASH64,HASH,XXH64,xxHash
