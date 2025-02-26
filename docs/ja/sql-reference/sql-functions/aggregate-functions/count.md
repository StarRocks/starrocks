---
displayed_sidebar: docs
---

# count

指定された式に基づいて、行の総数を返します。

この関数には3つのバリエーションがあります:

- `COUNT(*)` は、NULL 値を含むかどうかに関係なく、テーブル内のすべての行をカウントします。

- `COUNT(expr)` は、特定の列内の非 NULL 値を持つ行の数をカウントします。

- `COUNT(DISTINCT expr)` は、列内の重複排除された非 NULL 値の数をカウントします。

`COUNT(DISTINCT expr)` は、正確な重複排除カウントに使用されます。より高い重複排除カウントのパフォーマンスが必要な場合は、[Use bitmap for exact count discount](../../../using_starrocks/distinct_values/Using_bitmap.md)を参照してください。

StarRocks 2.4以降では、1つのステートメントで複数の COUNT(DISTINCT) を使用できます。

## Syntax

~~~Haskell
COUNT(expr)
COUNT(DISTINCT expr [,expr,...])`
~~~

## Parameters

`expr`: `count()` が実行される基準となる列または式。`expr` が列名の場合、その列は任意のデータ型でかまいません。

## Return value

数値を返します。行が見つからない場合は、0 が返されます。この関数は NULL 値を無視します。

## Examples

`test` という名前のテーブルがあるとします。`id` によって各注文の国、カテゴリ、サプライヤーをクエリします。

~~~Plain
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
~~~

Example 1: テーブル `test` の行数をカウントします。

~~~Plain
    select count(*) from test;
    +----------+
    | count(*) |
    +----------+
    |        7 |
    +----------+
~~~

Example 2: `id` 列の値の数をカウントします。

~~~Plain
    select count(id) from test;
    +-----------+
    | count(id) |
    +-----------+
    |         7 |
    +-----------+
~~~

Example 3: `category` 列の値の数を NULL 値を無視してカウントします。

~~~Plain
select count(category) from test;
  +-----------------+
  | count(category) |
  +-----------------+
  |         6       |
  +-----------------+
~~~

Example 4: `category` 列の重複排除された値の数をカウントします。

~~~Plain
select count(distinct category) from test;
+-------------------------+
| count(DISTINCT category) |
+-------------------------+
|                       4 |
+-------------------------+
~~~

Example 5: `category` と `supplier` の組み合わせの数をカウントします。

~~~Plain
select count(distinct category, supplier) from test;
+------------------------------------+
| count(DISTINCT category, supplier) |
+------------------------------------+
|                                  5 |
+------------------------------------+
~~~

出力では、`id` 1004 の組み合わせは `id` 1002 の組み合わせと重複しています。それらは一度だけカウントされます。`id` 1007 の組み合わせは NULL 値を含んでおり、カウントされません。

Example 6: 1つのステートメントで複数の COUNT(DISTINCT) を使用します。

~~~Plain
select count(distinct country, category), count(distinct country,supplier) from test;
+-----------------------------------+-----------------------------------+
| count(DISTINCT country, category) | count(DISTINCT country, supplier) |
+-----------------------------------+-----------------------------------+
|                                 6 |                                 7 |
+-----------------------------------+-----------------------------------+
~~~