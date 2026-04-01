---
displayed_sidebar: docs
sidebar_label: "LIMIT"
---

# LIMIT

LIMIT 句は、返される行の最大数を制限するために使用されます。返される行の最大数を設定すると、StarRocks がメモリ使用量を最適化するのに役立ちます。

この句は主に次のシナリオで使用されます。

Top-N クエリの結果を返します。

以下のテーブルに含まれる内容を検討してください。

テーブル内のデータ量が多いため、または WHERE 句がデータをあまりフィルタリングしないため、クエリ結果セットのサイズを制限する必要があります。

使用上の注意：LIMIT 句の値は、数値リテラル定数でなければなりません。

## 例

```plain text
mysql> select tiny_column from small_table limit 1;

+-------------+
|tiny_column  |
+-------------+
|     1       |
+-------------+

1 row in set (0.02 sec)
```

```plain text
mysql> select tiny_column from small_table limit 10000;

+-------------+
|tiny_column  |
+-------------+
|      1      |
|      2      |
+-------------+

2 rows in set (0.01 sec)
```
