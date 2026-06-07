---
displayed_sidebar: docs
description: "返回输入字符串的 XXH3 hash 派生的有符号 32 位值。"
---

# xx_hash3_32

返回输入字符串的 XXH3 hash 派生的有符号 32 位值。此函数使用 `XXH3_64bits_withSeed`，并将其低 32 位作为 `INT` 返回。对于多个输入，每个输入得到的 32 位结果会作为下一个输入的 seed。

## 语法

```Haskell
INT XX_HASH3_32(VARCHAR input, ...)
```

## 示例

```Plain Text
MySQL > select xx_hash3_32(null);
+-------------------+
| xx_hash3_32(NULL) |
+-------------------+
|              NULL |
+-------------------+
```

```Plain Text
MySQL > select xx_hash3_32("hello");
+----------------------+
| xx_hash3_32('hello') |
+----------------------+
|           1549982973 |
+----------------------+
```

```Plain Text
MySQL > select xx_hash3_32("hello", "world");
+-------------------------------+
| xx_hash3_32('hello', 'world') |
+-------------------------------+
|                   -1872669326 |
+-------------------------------+
```

## 相关函数

- [`xx_hash3_64`](./xx_hash3_64.md)
- [`xx_hash32`](./xx_hash32.md)

## keyword

XX_HASH3_32,HASH,xxHash3
