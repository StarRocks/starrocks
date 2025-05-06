---
displayed_sidebar: docs
---

# murmur_hash3_32

## 説明

入力文字列の32ビットのmurmur3ハッシュ値を返します。

## 構文

```Haskell
INT MURMUR_HASH3_32(VARCHAR input, ...)
```

## 例

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

## キーワード

MURMUR_HASH3_32,HASH