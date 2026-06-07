---
displayed_sidebar: docs
description: "入力文字列のXXH3ハッシュから派生した符号付き32ビット値を返します。"
---

# xx_hash3_32

入力文字列のXXH3ハッシュから派生した符号付き32ビット値を返します。この関数は`XXH3_64bits_withSeed`を使用し、その下位32ビットを`INT`として返します。複数の入力では、各入力から得られた32ビット結果が次の入力のseedとして使用されます。

## 構文

```Haskell
INT XX_HASH3_32(VARCHAR input, ...)
```

## 例

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

## 関連関数

- [`xx_hash3_64`](./xx_hash3_64.md)
- [`xx_hash32`](./xx_hash32.md)

## キーワード

XX_HASH3_32,HASH,xxHash3
