---
displayed_sidebar: docs
---

# xx_hash3_64

## 説明

入力文字列の64ビットのxxhash3ハッシュ値を返します。

## 構文

```Haskell
BIGINT XX_HASH3_64(VARCHAR input, ...)
```

## 例

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

## キーワード

XX_HASH3_64,HASH