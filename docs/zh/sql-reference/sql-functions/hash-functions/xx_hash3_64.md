---
displayed_sidebar: docs
---

# xx_hash3_64

## 功能

返回输入字符串的 64 位 xxhash3 hash 值。xx_hash3_64 使用 AVX2 指令集，提供比 [murmur_hash3_32](./murmur_hash3_32.md) 更快的速度和更优的性能。

该函数从 3.2.0 版本开始支持。

## 语法

```Haskell
BIGINT XX_HASH3_64(VARCHAR input, ...)
```

## 参数说明

`input`: 支持的数据类型为 VARCHAR。

## 返回值说明

返回值的数据类型为 BIGINT。如果输入值为 NULL，则返回 NULL。

## 示例

```Plain Text
MySQL > select xx_hash3_64(null);
+-------------------+
| xx_hash3_64(NULL) |
+-------------------+
|              NULL |
+-------------------+

MySQL > select xx_hash3_64("hello");
+----------------------+
| xx_hash3_64('hello') |
+----------------------+
| -7685981735718036227 |
+----------------------+

MySQL > select xx_hash3_64("hello", "world");
+-------------------------------+
| xx_hash3_64('hello', 'world') |
+-------------------------------+
|           7001965798170371843 |
+-------------------------------+
```

## keyword

XX_HASH3_64,HASH,xxHash3
