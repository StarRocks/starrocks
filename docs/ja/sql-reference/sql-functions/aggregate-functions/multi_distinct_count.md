---
displayed_sidebar: docs
---

# multi_distinct_count

`expr` の行数の合計を返します。これは count(distinct expr) と同等です。

## Syntax

```Haskell
multi_distinct_count(expr)
```

## Parameters

`expr`: `multi_distinct_count()` が実行される基準となる列または式です。`expr` が列名の場合、その列は任意のデータ型でかまいません。

## Return value

数値を返します。行が見つからない場合は 0 が返されます。この関数は NULL 値を無視します。

## Examples

`test` という名前のテーブルがあるとします。`id` で各注文のカテゴリとサプライヤーをクエリします。

```Plain
select * from test order by id;
+------+----------+----------+------------+
| id   | country  | category | supplier   |
+------+----------+----------+------------+
| 1001 | US       | A        | supplier_1 |
| 1002 | Thailand | A        | supplier_2 |
| 1003 | Turkey   | B        | supplier_3 |
| 1004 | US       | A        | supplier_2 |
| 1005 | China    | C        | supplier_4 |
| 1006 | Japan    | D        | supplier_3 |
| 1007 | Japan    | NULL     | supplier_5 |
+------+----------+----------+------------+
```

Example 1: `category` 列の異なる値の数を数えます。

```Plain
select multi_distinct_count(category) from test;
+--------------------------------+
| multi_distinct_count(category) |
+--------------------------------+
|                              4 |
+--------------------------------+
```

Example 2: `supplier` 列の異なる値の数を数えます。

```Plain
select multi_distinct_count(supplier) from test;
+--------------------------------+
| multi_distinct_count(supplier) |
+--------------------------------+
|                              5 |
+--------------------------------+
```