---
displayed_sidebar: docs
---

# crc32

文字列の32ビット循環冗長検査 (CRC) 値を返します。入力が NULL の場合、NULL が返されます。

この関数はエラー検出に使用されます。CRC32 アルゴリズムを使用して、ソースデータとターゲットデータの間の変更を検出します。

この関数は v3.3 以降でサポートされています。

## 構文

```Haskell
BIGINT crc32(VARCHAR str)
```

## パラメータ

`str`: CRC32 値を計算したい文字列。

## 例

```Plain Text
mysql > select crc32("starrocks");
+--------------------+
| crc32('starrocks') |
+--------------------+
|         2312449062 |
+--------------------+

mysql > select crc32(null);
+-------------+
| crc32(NULL) |
+-------------+
|        NULL |
+-------------+
1 row in set (0.18 sec)
```

## キーワード

CRC32