---
displayed_sidebar: docs
sidebar_label: "INTERSECT"
---

# INTERSECT

複数のクエリの結果の共通部分、つまりすべての結果セットに現れる結果を計算します。この句は、結果セットの中から一意の行のみを返します。ALL キーワードはサポートされていません。

## 構文

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **NOTE**
>
> - INTERSECT は INTERSECT DISTINCT と同等です。
> - 各クエリステートメントは、同じ数のカラムを返し、カラムは互換性のあるデータ型を持っている必要があります。

## 例

UNION の 2 つのテーブルが使用されます。

両方のテーブルに共通する個別の `(id, price)` の組み合わせを返します。次の 2 つのステートメントは同等です。

```Plaintext
mysql> (select id, price from select1) intersect (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+

mysql> (select id, price from select1) intersect distinct (select id, price from select2)
order by id;

+------+-------+
| id   | price |
+------+-------+
|    2 |     3 |
|    5 |     6 |
+------+-------+
```
