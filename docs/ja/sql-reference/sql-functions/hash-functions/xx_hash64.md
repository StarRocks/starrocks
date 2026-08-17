---
displayed_sidebar: docs
description: "入力文字列の64ビットのXXH64ハッシュ値を返します。"
---

# xx_hash64

入力文字列の64ビットのXXH64ハッシュ値を返します。

## 構文

```Haskell
BIGINT XX_HASH64(VARCHAR input, ...)
```

## 例

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

## 関連関数

- [`xx_hash32`](./xx_hash32.md)
- [`xx_hash3_64`](./xx_hash3_64.md)

## keyword

XX_HASH64,HASH,XXH64,xxHash
