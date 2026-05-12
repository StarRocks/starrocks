---
displayed_sidebar: docs
sidebar_label: "PIVOT"
---

# PIVOT

この機能は v3.3 以降でサポートされています。

PIVOT 操作は SQL の高度な機能であり、テーブル内の行を列に変換できます。これは、ピボットテーブルの作成に特に役立ちます。これは、データベースレポートや分析を扱う場合、特にプレゼンテーション用にデータを要約または分類する必要がある場合に役立ちます。

実際、PIVOT はシンタックスシュガーであり、`sum(case when ... then ... end)` のようなクエリステートメントの記述を簡素化できます。

## 構文

```sql
pivot:
SELECT ...
FROM ...
PIVOT (
  aggregate_function(<expr>) [[AS] alias] [, aggregate_function(<expr>) [[AS] alias] ...]
  FOR <pivot_column>
  IN (<pivot_value>)
)

pivot_column:
<column_name> 
| (<column_name> [, <column_name> ...])

pivot_value:
<literal> [, <literal> ...]
| (<literal>, <literal> ...) [, (<literal>, <literal> ...)]
```

## パラメータ

PIVOT 操作では、いくつかの重要なコンポーネントを指定する必要があります。

- aggregate_function(): データの集計に使用される SUM、AVG、COUNT などの集計関数。
- alias: 集計結果のエイリアス。結果をより理解しやすくします。
- FOR pivot_column: 行から列への変換を実行する列名を指定します。
- IN (pivot_value): 列に変換される pivot_column の特定の値​​を指定します。

## 例

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- The result is equivalent to the following query:
SELECT SUM(CASE WHEN c3 = 1 THEN c1 ELSE NULL END) AS sum_c1_1,
       AVG(CASE WHEN c3 = 1 THEN c2 ELSE NULL END) AS avg_c2_1,
       SUM(CASE WHEN c3 = 2 THEN c1 ELSE NULL END) AS sum_c1_2,
       AVG(CASE WHEN c3 = 2 THEN c2 ELSE NULL END) AS avg_c2_2,
       SUM(CASE WHEN c3 = 3 THEN c1 ELSE NULL END) AS sum_c1_3,
       AVG(CASE WHEN c3 = 3 THEN c2 ELSE NULL END) AS avg_c2_3,
       SUM(CASE WHEN c3 = 4 THEN c1 ELSE NULL END) AS sum_c1_4,
       AVG(CASE WHEN c3 = 4 THEN c2 ELSE NULL END) AS avg_c2_4,
       SUM(CASE WHEN c3 = 5 THEN c1 ELSE NULL END) AS sum_c1_5,
       AVG(CASE WHEN c3 = 5 THEN c2 ELSE NULL END) AS avg_c2_5
FROM t1
GROUP BY c0;
```
