---
displayed_sidebar: docs
sidebar_label: "EXCEPT/MINUS"
---

# EXCEPT/MINUS

左側のクエリの結果のうち、右側のクエリに存在しない重複を除いた結果を返します。EXCEPT は MINUS と同等です。

## 構文

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **NOTE**
>
> - EXCEPT は EXCEPT DISTINCT と同等です。 ALL キーワードはサポートされていません。
> - 各クエリステートメントは、同じ数のカラムを返し、カラムは互換性のあるデータ型でなければなりません。

## 例

UNION の 2 つのテーブルが使用されます。

`select1` にあり、`select2` にはない個別の `(id, price)` の組み合わせを返します。

```Plaintext
mysql> (select id, price from select1) except (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+

mysql> (select id, price from select1) minus (select id, price from select2)
order by id;
+------+-------+
| id   | price |
+------+-------+
|    1 |     2 |
+------+-------+
```
