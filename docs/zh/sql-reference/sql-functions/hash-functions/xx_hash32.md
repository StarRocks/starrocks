---
displayed_sidebar: docs
description: "返回输入字符串的 32 位 XXH32 hash 值。"
---

# xx_hash32

返回输入字符串的 32 位 XXH32 hash 值。

## 语法

```Haskell
INT XX_HASH32(VARCHAR input, ...)
```

## 示例

```Plain Text
MySQL > select xx_hash32(null);
+-----------------+
| xx_hash32(NULL) |
+-----------------+
|            NULL |
+-----------------+
```

```Plain Text
MySQL > select xx_hash32("hello");
+--------------------+
| xx_hash32('hello') |
+--------------------+
|          -83855367 |
+--------------------+
```

```Plain Text
MySQL > select xx_hash32("hello", "world");
+-----------------------------+
| xx_hash32('hello', 'world') |
+-----------------------------+
|                  -920844969 |
+-----------------------------+
```

## 相关函数

- [`xx_hash64`](./xx_hash64.md)
- [`xx_hash3_64`](./xx_hash3_64.md)

## keyword

XX_HASH32,HASH,XXH32,xxHash
