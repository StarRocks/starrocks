---
displayed_sidebar: docs
---

# hll_hash

値を hll 型に変換します。通常、インポート時にソースデータの値を StarRocks テーブルの HLL 列型にマッピングするために使用されます。

## 構文

```Haskell
HLL_HASH(column_name)
```

## パラメータ

`column_name`: 生成された HLL 列の名前。

## 戻り値

HLL 型の値を返します。

## 例

```plain text
mysql> select hll_cardinality(hll_hash("a"));
+--------------------------------+
| hll_cardinality(hll_hash('a')) |
+--------------------------------+
|                              1 |
+--------------------------------+
```