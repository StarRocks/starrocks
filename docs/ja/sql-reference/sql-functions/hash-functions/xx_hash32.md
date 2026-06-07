---
displayed_sidebar: docs
description: "入力文字列の32ビットのXXH32ハッシュ値を返します。"
---

# xx_hash32

入力文字列の32ビットのXXH32ハッシュ値を返します。

## 構文

```Haskell
INT XX_HASH32(VARCHAR input, ...)
```

## 例

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

## 関連関数

- [`xx_hash64`](./xx_hash64.md)
- [`xx_hash3_32`](./xx_hash3_32.md)

## keyword

XX_HASH32,HASH,XXH32,xxHash
