---
displayed_sidebar: docs
sidebar_position: 0.9
keywords: ['analytic']
---

# ウィンドウ関数

- [ウィンドウ関数](#window-functions)
  - [背景](#background)
  - [使用法](#usage)
    - [構文](#syntax)
    - [PARTITION BY句](#partition-by-clause)
    - [ORDER BY句](#order-by-clause)
    - [ウィンドウ句](#window-clause)
  - [ウィンドウ関数のサンプルテーブル](#window-function-sample-table)
  - [関数の例](#function-examples)
    - [AVG()](#avg)
    - [COUNT()](#count)
    - [CUME\_DIST()](#cume_dist)
    - [DENSE\_RANK()](#dense_rank)
    - [FIRST\_VALUE()](#first_value)
    - [LAST\_VALUE()](#last_value)
    - [LAG()](#lag)
    - [LEAD()](#lead)
    - [MAX()](#max)
    - [MIN()](#min)
    - [NTILE()](#ntile)
    - [PERCENT\_RANK()](#percent_rank)
    - [RANK()](#rank)
    - [ROW\_NUMBER()](#row_number)
    - [QUALIFY()](#qualify)
    - [SUM()](#sum)
    - [VARIANCE, VAR\_POP, VARIANCE\_POP](#variance-var_pop-variance_pop)
    - [VAR\_SAMP, VARIANCE\_SAMP](#var_samp-variance_samp)
    - [STD, STDDEV, STDDEV\_POP](#std-stddev-stddev_pop)
    - [STDDEV\_SAMP](#stddev_samp)
    - [COVAR\_SAMP](#covar_samp)
    - [COVAR\_POP](#covar_pop)
    - [CORR](#corr)

## 背景

ウィンドウ関数は、特別なクラスの組み込み関数です。集計関数と同様に、複数の入力行に対して計算を行い、単一のデータ値を取得します。違いは、ウィンドウ関数が特定のウィンドウ内で入力データを処理することであり、「group by」メソッドを使用するのではありません。各ウィンドウ内のデータは、over()句を使用してソートおよびグループ化できます。ウィンドウ関数は、各行に対して個別の値を計算するため、グループごとに1つの値を計算するのではありません。この柔軟性により、ユーザーはselect句に追加の列を追加し、結果セットをさらにフィルタリングすることができます。ウィンドウ関数は、selectリストと句の最外部の位置にのみ表示されることができます。クエリの最後に効果を発揮し、つまり、`join`、`where`、および`group by`操作が実行された後に適用されます。ウィンドウ関数は、トレンドの分析、外れ値の計算、大規模データのバケッティング分析にしばしば使用されます。

## 使用法

### 構文

```SQL
function(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### PARTITION BY句

Partition By句はGroup Byに似ています。指定された1つ以上の列で入力行をグループ化します。同じ値を持つ行は一緒にグループ化されます。

### ORDER BY句

`Order By`句は基本的に外部の`Order By`と同じです。入力行の順序を定義します。`Partition By`が指定されている場合、`Order By`は各Partitionグループ内の順序を定義します。唯一の違いは、`OVER`句の`Order By n`（nは正の整数）が操作なしに等しいのに対し、外部の`Order By`の`n`はn番目の列でのソートを示すことです。

例:

この例では、eventsテーブルの`date_and_time`列でソートされた1, 2, 3などの値を持つid列をselectリストに追加します。

```SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
```

### ウィンドウ句

ウィンドウ句は、操作のための行の範囲を指定するために使用されます（現在の行に基づく前後の行）。次の構文をサポートしています: AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE(), および SUM()。MAX() および MIN() の場合、ウィンドウ句は `UNBOUNDED PRECEDING` までの開始を指定できます。

**構文:**

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

## ウィンドウ関数のサンプルテーブル

このセクションでは、サンプルテーブル`scores`を作成します。このテーブルを使用して、以下の多くのウィンドウ関数をテストできます。

```SQL
CREATE TABLE `scores` (
    `id` int(11) NULL,
    `name` varchar(11) NULL,
    `subject` varchar(11) NULL,
    `score` int(11) NULL
  )
DISTRIBUTED BY HASH(`score`) BUCKETS 10;

INSERT INTO `scores` VALUES
  (1, "lily", "math", NULL),
  (1, "lily", "english", 100),
  (1, "lily", "physics", 60),
  (2, "tom", "math", 80),
  (2, "tom", "english", 98),
  (2, "tom", "physics", NULL),
  (3, "jack", "math", 95),
  (3, "jack", "english", NULL),
  (3, "jack", "physics", 99),
  (4, "amy", "math", 80),
  (4, "amy", "english", 92),
  (4, "amy", "physics", 99),
  (5, "mike", "math", 70),
  (5, "mike", "english", 85),
  (5, "mike", "physics", 85),
  (6, "amber", "math", 92),
  (6, "amber", NULL, 90),
  (6, "amber", "physics", 100);
```

## 関数の例

このセクションでは、StarRocksでサポートされているウィンドウ関数について説明します。

### AVG()

指定されたウィンドウ内のフィールドの平均値を計算します。この関数はNULL値を無視します。

**構文:**

```SQL
AVG(expr) [OVER (*analytic_clause*)]
```

**例:**

次の例では、株式データを例として使用します。

```SQL
CREATE TABLE stock_ticker (
    stock_symbol  STRING,
    closing_price DECIMAL(8,2),
    closing_date  DATETIME
)
DUPLICATE KEY(stock_symbol)
COMMENT "OLAP"
DISTRIBUTED BY HASH(closing_date);

INSERT INTO stock_ticker VALUES 
    ("JDR", 12.86, "2014-10-02 00:00:00"), 
    ("JDR", 12.89, "2014-10-03 00:00:00"), 
    ("JDR", 12.94, "2014-10-04 00:00:00"), 
    ("JDR", 12.55, "2014-10-05 00:00:00"), 
    ("JDR", 14.03, "2014-10-06 00:00:00"), 
    ("JDR", 14.75, "2014-10-07 00:00:00"), 
    ("JDR", 13.98, "2014-10-08 00:00:00")
;
```

現在の行とその前後の行の平均終値を計算します。

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

出力:

```plaintext
+--------------+---------------------+---------------+----------------+
| stock_symbol | closing_date        | closing_price | moving_average |
+--------------+---------------------+---------------+----------------+
| JDR          | 2014-10-02 00:00:00 |         12.86 |    12.87500000 |
| JDR          | 2014-10-03 00:00:00 |         12.89 |    12.89666667 |
| JDR          | 2014-10-04 00:00:00 |         12.94 |    12.79333333 |
| JDR          | 2014-10-05 00:00:00 |         12.55 |    13.17333333 |
| JDR          | 2014-10-06 00:00:00 |         14.03 |    13.77666667 |
| JDR          | 2014-10-07 00:00:00 |         14.75 |    14.25333333 |
| JDR          | 2014-10-08 00:00:00 |         13.98 |    14.36500000 |
+--------------+---------------------+---------------+----------------+
```

例えば、最初の行の`12.87500000`は、"2014-10-02"の終値（`12.86`）、その前日"2014-10-01"（null）、およびその翌日"2014-10-03"（`12.89`）の平均値です。

### COUNT()

指定されたウィンドウ内で条件を満たす行の総数を計算します。

**構文:**

```SQL
COUNT(expr) [OVER (analytic_clause)]
```

**例:**

mathパーティション内で、現在の行から最初の行までのmathスコアが90を超える出現回数をカウントします。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    count(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and current row
        ) as 'score_count'
from scores where subject in ('math') and score > 90;
```

```plaintext
+------+-------+---------+-------+-------------+
| id   | name  | subject | score | score_count |
+------+-------+---------+-------+-------------+
|    6 | amber | math    |    92 |           1 |
|    3 | jack  | math    |    95 |           2 |
+------+-------+---------+-------+-------------+
```

### CUME_DIST()

CUME_DIST()関数は、パーティションまたはウィンドウ内での値の累積分布を計算し、その相対的な位置をパーティション内のパーセンテージとして示します。グループ内の最高値または最低値の分布を計算するために使用されます。

- データが昇順にソートされている場合、この関数は現在の行の値以下の値のパーセンテージを計算します。
- データが降順にソートされている場合、この関数は現在の行の値以上の値のパーセンテージを計算します。

累積分布は0から1の範囲にあります。パーセンタイル計算やデータ分布分析に役立ちます。

この関数はv3.2からサポートされています。

**構文:**

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

- `partition_by_clause`: 任意。指定しない場合、結果セット全体が単一のパーティションとして処理されます。
- `order_by_clause`: **この関数は、パーティション行を希望の順序にソートするためにORDER BYと一緒に使用する必要があります。**

CUME_DIST()はNULL値を含み、これらを最低値として扱います。

**例:**

次の例では、各`subject`グループ内の各スコアの累積分布を示します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
SELECT *, 
    cume_dist() 
      OVER (
        PARTITION BY subject
        ORDER BY score
      ) AS cume_dist 
FROM scores;
+------+-------+---------+-------+---------------------+
| id   | name  | subject | score | cume_dist           |
+------+-------+---------+-------+---------------------+
|    6 | amber | NULL    |    90 |                   1 |
|    3 | jack  | english |  NULL |                 0.2 |
|    5 | mike  | english |    85 |                 0.4 |
|    4 | amy   | english |    92 |                 0.6 |
|    2 | tom   | english |    98 |                 0.8 |
|    1 | lily  | english |   100 |                   1 |
|    1 | lily  | math    |  NULL | 0.16666666666666666 |
|    5 | mike  | math    |    70 |  0.3333333333333333 |
|    2 | tom   | math    |    80 |  0.6666666666666666 |
|    4 | amy   | math    |    80 |  0.6666666666666666 |
|    6 | amber | math    |    92 |  0.8333333333333334 |
|    3 | jack  | math    |    95 |                   1 |
|    2 | tom   | physics |  NULL | 0.16666666666666666 |
|    1 | lily  | physics |    60 |  0.3333333333333333 |
|    5 | mike  | physics |    85 |                 0.5 |
|    4 | amy   | physics |    99 |  0.8333333333333334 |
|    3 | jack  | physics |    99 |  0.8333333333333334 |
|    6 | amber | physics |   100 |                   1 |
+------+-------+---------+-------+---------------------+
```

- 最初の行の`cume_dist`について、`NULL`グループには1行しかなく、この行自体のみが「現在の行以下」の条件を満たしています。累積分布は1です。
- 2行目の`cume_dist`について、`english`グループには5行あり、この行自体（NULL）のみが「現在の行以下」の条件を満たしています。累積分布は0.2です。
- 3行目の`cume_dist`について、`english`グループには5行あり、2行（85とNULL）が「現在の行以下」の条件を満たしています。累積分布は0.4です。

### DENSE_RANK()

DENSE_RANK()関数はランキングを表現するために使用されます。RANK()とは異なり、DENSE_RANK()は**空白のない**番号を持ちます。例えば、1が2つある場合、DENSE_RANK()の3番目の番号は2のままですが、RANK()の3番目の番号は3になります。

**構文:**

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

**例:**

次の例では、mathスコアのランキング（降順）を示します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    dense_rank()
        over (
            partition by subject
            order by score desc
        ) as `rank`
from scores where subject in ('math');
```

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    2 | tom   | math    |    80 |    3 |
|    4 | amy   | math    |    80 |    3 |
|    5 | mike  | math    |    70 |    4 |
|    1 | lily  | math    |  NULL |    5 |
+------+-------+---------+-------+------+
```

結果データには、スコアが80の行が2つあります。これらはすべて3位です。次のスコア70のランクは4です。これにより、DENSE_RANK()が**空白のない**番号を持つことが示されています。

### FIRST_VALUE()

FIRST_VALUE()はウィンドウ範囲の**最初の**値を返します。

**構文:**

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0からサポートされています。これは、`expr`のNULL値を計算から除外するかどうかを決定するために使用されます。デフォルトでは、NULL値が含まれており、フィルタリングされた結果の最初の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定すると、フィルタリングされた結果の最初の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

**例:**

各グループ（降順）で、`subject`でグループ化された各メンバーの最初の`score`値を返します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    first_value(score IGNORE NULLS)
        over (
            partition by subject
            order by score desc
        ) as first
from scores;
```

```plaintext
+------+-------+---------+-------+-------+
| id   | name  | subject | score | first |
+------+-------+---------+-------+-------+
|    1 | lily  | english |   100 |   100 |
|    2 | tom   | english |    98 |   100 |
|    4 | amy   | english |    92 |   100 |
|    5 | mike  | english |    85 |   100 |
|    3 | jack  | english |  NULL |   100 |
|    6 | amber | physics |   100 |   100 |
|    3 | jack  | physics |    99 |   100 |
|    4 | amy   | physics |    99 |   100 |
|    5 | mike  | physics |    85 |   100 |
|    1 | lily  | physics |    60 |   100 |
|    2 | tom   | physics |  NULL |   100 |
|    6 | amber | NULL    |    90 |    90 |
|    3 | jack  | math    |    95 |    95 |
|    6 | amber | math    |    92 |    95 |
|    2 | tom   | math    |    80 |    95 |
|    4 | amy   | math    |    80 |    95 |
|    5 | mike  | math    |    70 |    95 |
|    1 | lily  | math    |  NULL |    95 |
+------+-------+---------+-------+-------+
```

### LAST_VALUE()

LAST_VALUE()はウィンドウ範囲の**最後の**値を返します。これはFIRST_VALUE()の逆です。

**構文:**

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0からサポートされています。これは、`expr`のNULL値を計算から除外するかどうかを決定するために使用されます。デフォルトでは、NULL値が含まれており、フィルタリングされた結果の最後の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定すると、フィルタリングされた結果の最後の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

デフォルトでは、LAST_VALUE()は`rows between unbounded preceding and current row`を計算し、現在の行とその前のすべての行を比較します。各パーティションに1つの値のみを表示する場合は、ORDER BYの後に`rows between unbounded preceding and unbounded following`を使用します。

**例:**

各グループ（降順）で、`subject`でグループ化された各メンバーの最後の`score`を返します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    last_value(score IGNORE NULLS)
        over (
            partition by subject
            order by score desc
            rows between unbounded preceding and unbounded following
        ) as last
from scores;
```

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | last |
+------+-------+---------+-------+------+
|    1 | lily  | english |   100 |   85 |
|    2 | tom   | english |    98 |   85 |
|    4 | amy   | english |    92 |   85 |
|    5 | mike  | english |    85 |   85 |
|    3 | jack  | english |  NULL |   85 |
|    6 | amber | physics |   100 |   60 |
|    3 | jack  | physics |    99 |   60 |
|    4 | amy   | physics |    99 |   60 |
|    5 | mike  | physics |    85 |   60 |
|    1 | lily  | physics |    60 |   60 |
|    2 | tom   | physics |  NULL |   60 |
|    6 | amber | NULL    |    90 |   90 |
|    3 | jack  | math    |    95 |   70 |
|    6 | amber | math    |    92 |   70 |
|    2 | tom   | math    |    80 |   70 |
|    4 | amy   | math    |    80 |   70 |
|    5 | mike  | math    |    70 |   70 |
|    1 | lily  | math    |  NULL |   70 |
+------+-------+---------+-------+------+
```

### LAG()

現在の行から`offset`行遅れた行の値を返します。この関数は、行間の値を比較し、データをフィルタリングするためによく使用されます。

`LAG()`は、次のタイプのデータをクエリするために使用できます:

- 数値: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
- 文字列: CHAR, VARCHAR
- 日付: DATE, DATETIME
- BITMAPおよびHLLはStarRocks v2.5からサポートされています。

**構文:**

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**パラメータ:**

- `expr`: 計算したいフィールド。
- `offset`: オフセット。**正の整数**でなければなりません。このパラメータが指定されない場合、デフォルトは1です。
- `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されない場合、デフォルトはNULLです。`default`は`expr`と互換性のあるタイプの任意の式をサポートします。
- `IGNORE NULLS`はv3.0からサポートされています。これは、`expr`のNULL値が結果に含まれるかどうかを決定するために使用されます。デフォルトでは、`offset`行がカウントされるときにNULL値が含まれており、目的の行の値がNULLの場合、NULLが返されます。例1を参照してください。IGNORE NULLSを指定すると、`offset`行がカウントされるときにNULL値が無視され、システムは`offset`の非NULL値を探し続けます。`offset`の非NULL値が見つからない場合、NULLまたは`default`（指定されている場合）が返されます。例2を参照してください。

**例1: IGNORE NULLSが指定されていない場合**

テーブルを作成し、値を挿入します:

```SQL
CREATE TABLE test_tbl (col_1 INT, col_2 INT)
DISTRIBUTED BY HASH(col_1);

INSERT INTO test_tbl VALUES 
    (1, NULL),
    (2, 4),
    (3, NULL),
    (4, 2),
    (5, NULL),
    (6, 7),
    (7, 6),
    (8, 5),
    (9, NULL),
    (10, NULL);
```

このテーブルからデータをクエリし、`offset`は2で、これは前の2行をトラバースすることを意味します。`default`は0で、一致する行が見つからない場合に0が返されます。

出力:

```plaintext
SELECT col_1, col_2, LAG(col_2,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+---------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+---------------------------------------------+
|     1 |  NULL |                                           0 |
|     2 |     4 |                                           0 |
|     3 |  NULL |                                        NULL |
|     4 |     2 |                                           4 |
|     5 |  NULL |                                        NULL |
|     6 |     7 |                                           2 |
|     7 |     6 |                                        NULL |
|     8 |     5 |                                           7 |
|     9 |  NULL |                                           6 |
|    10 |  NULL |                                           5 |
+-------+-------+---------------------------------------------+
```

最初の2行には、前の2行が存在せず、デフォルト値0が返されます。

3行目のNULLについては、2行前の値がNULLであり、NULLが返されます。なぜなら、NULL値が許可されているからです。

**例2: IGNORE NULLSが指定されている場合**

前述のテーブルとパラメータ設定を使用します。

```SQL
SELECT col_1, col_2, LAG(col_2 IGNORE NULLS,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+---------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+---------------------------------------------+
|     1 |  NULL |                                           0 |
|     2 |     4 |                                           0 |
|     3 |  NULL |                                           0 |
|     4 |     2 |                                           0 |
|     5 |  NULL |                                           4 |
|     6 |     7 |                                           4 |
|     7 |     6 |                                           2 |
|     8 |     5 |                                           7 |
|     9 |  NULL |                                           6 |
|    10 |  NULL |                                           6 |
+-------+-------+---------------------------------------------+
```

1行目から4行目まで、システムは前の行で2つの非NULL値を見つけることができず、デフォルト値0が返されます。

7行目の値6については、2行前の値がNULLであり、IGNORE NULLSが指定されているため、NULLが無視されます。システムは非NULL値を探し続け、4行目の2が返されます。

### LEAD()

現在の行から`offset`行進んだ行の値を返します。この関数は、行間の値を比較し、データをフィルタリングするためによく使用されます。

`LEAD()`でクエリできるデータ型は、[LAG()](#lag)でサポートされているものと同じです。

**構文:**

```sql
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**パラメータ:**

- `expr`: 計算したいフィールド。
- `offset`: オフセット。正の整数でなければなりません。このパラメータが指定されない場合、デフォルトは1です。
- `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されない場合、デフォルトはNULLです。`default`は`expr`と互換性のあるタイプの任意の式をサポートします。
- `IGNORE NULLS`はv3.0からサポートされています。これは、`expr`のNULL値が結果に含まれるかどうかを決定するために使用されます。デフォルトでは、`offset`行がカウントされるときにNULL値が含まれており、目的の行の値がNULLの場合、NULLが返されます。例1を参照してください。IGNORE NULLSを指定すると、`offset`行がカウントされるときにNULL値が無視され、システムは`offset`の非NULL値を探し続けます。`offset`の非NULL値が見つからない場合、NULLまたは`default`（指定されている場合）が返されます。例2を参照してください。

**例1: IGNORE NULLSが指定されていない場合**

テーブルを作成し、値を挿入します:

```SQL
CREATE TABLE test_tbl (col_1 INT, col_2 INT)
DISTRIBUTED BY HASH(col_1);

INSERT INTO test_tbl VALUES 
    (1, NULL),
    (2, 4),
    (3, NULL),
    (4, 2),
    (5, NULL),
    (6, 7),
    (7, 6),
    (8, 5),
    (9, NULL),
    (10, NULL);
```

このテーブルからデータをクエリし、`offset`は2で、これは次の2行をトラバースすることを意味します。`default`は0で、一致する行が見つからない場合に0が返されます。

出力:

```plaintext
SELECT col_1, col_2, LEAD(col_2,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+----------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+----------------------------------------------+
|     1 |  NULL |                                         NULL |
|     2 |     4 |                                            2 |
|     3 |  NULL |                                         NULL |
|     4 |     2 |                                            7 |
|     5 |  NULL |                                            6 |
|     6 |     7 |                                            5 |
|     7 |     6 |                                         NULL |
|     8 |     5 |                                         NULL |
|     9 |  NULL |                                            0 |
|    10 |  NULL |                                            0 |
+-------+-------+----------------------------------------------+
```

最初の行については、2行先の値がNULLであり、NULLが返されます。なぜなら、NULL値が許可されているからです。

最後の2行については、次の2行が存在せず、デフォルト値0が返されます。

**例2: IGNORE NULLSが指定されている場合**

前述のテーブルとパラメータ設定を使用します。

```SQL
SELECT col_1, col_2, LEAD(col_2 IGNORE NULLS,2,0) OVER (ORDER BY col_1) 
FROM test_tbl ORDER BY col_1;
+-------+-------+----------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, 0) OVER (ORDER BY col_1 ASC ) |
+-------+-------+----------------------------------------------+
|     1 |  NULL |                                            2 |
|     2 |     4 |                                            7 |
|     3 |  NULL |                                            7 |
|     4 |     2 |                                            6 |
|     5 |  NULL |                                            6 |
|     6 |     7 |                                            5 |
|     7 |     6 |                                            0 |
|     8 |     5 |                                            0 |
|     9 |  NULL |                                            0 |
|    10 |  NULL |                                            0 |
+-------+-------+----------------------------------------------+
```

7行目から10行目まで、システムは次の行で2つの非NULL値を見つけることができず、デフォルト値0が返されます。

最初の行については、2行先の値がNULLであり、IGNORE NULLSが指定されているため、NULLが無視されます。システムは2番目の非NULL値を探し続け、4行目の2が返されます。

### MAX()

現在のウィンドウ内の指定された行の最大値を返します。

**構文:**

```SQL
MAX(expr) [OVER (analytic_clause)]
```

**例:**

現在の行から次の行までの行の最大値を計算します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    max(scores)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and 1 following
        ) as max
from scores
where subject in ('math');
```

```plain
+------+-------+---------+-------+------+
| id   | name  | subject | score | max  |
+------+-------+---------+-------+------+
|    1 | lily  | math    |  NULL |   70 |
|    5 | mike  | math    |    70 |   80 |
|    2 | tom   | math    |    80 |   80 |
|    4 | amy   | math    |    80 |   92 |
|    6 | amber | math    |    92 |   95 |
|    3 | jack  | math    |    95 |   95 |
+------+-------+---------+-------+------+
```

次の例では、`math`科目のすべての行の最大スコアを計算します。

```sql
select *,
    max(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following
        ) as max
from scores
where subject in ('math');
```

StarRocks 2.4以降では、行範囲を`rows between n preceding and n following`として指定でき、これは現在の行の前の`n`行と後の`n`行をキャプチャできることを意味します。

例文:

```sql
select *,
    max(score)
        over (
            partition by subject
            order by score
            rows between 3 preceding and 2 following) as max
from scores
where subject in ('math');
```

### MIN()

現在のウィンドウ内の指定された行の最小値を返します。

**構文:**

```SQL
MIN(expr) [OVER (analytic_clause)]
```

**例:**

math科目のすべての行の最低スコアを計算します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *, 
    min(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following)
            as min
from scores
where subject in ('math');
```

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | min  |
+------+-------+---------+-------+------+
|    1 | lily  | math    |  NULL |   70 |
|    5 | mike  | math    |    70 |   70 |
|    2 | tom   | math    |    80 |   70 |
|    4 | amy   | math    |    80 |   70 |
|    6 | amber | math    |    92 |   70 |
|    3 | jack  | math    |    95 |   70 |
+------+-------+---------+-------+------+
```

StarRocks 2.4以降では、行範囲を`rows between n preceding and n following`として指定でき、これは現在の行の前の`n`行と後の`n`行をキャプチャできることを意味します。

例文:

```SQL
select *,
    min(score)
        over (
            partition by subject
            order by score
            rows between 3 preceding and 2 following) as max
from scores
where subject in ('math');
```

### NTILE()

NTILE()関数は、パーティション内のソートされた行を指定された`num_buckets`の数でできるだけ均等に分割し、それぞれのバケットに分割された行を1から始まる番号`[1, 2, ..., num_buckets]`に格納し、各行が属するバケット番号を返します。

バケットのサイズについて:

- 行数が指定された`num_buckets`の数で正確に割り切れる場合、すべてのバケットは同じサイズになります。
- 行数が指定された`num_buckets`の数で正確に割り切れない場合、2つの異なるサイズのバケットが存在します。サイズの差は1です。より多くの行を持つバケットは、より少ない行を持つバケットの前にリストされます。

**構文:**

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`: 作成されるバケットの数。値は定数の正の整数で、最大は`2^63 - 1`です。

NTILE()関数ではウィンドウ句は許可されていません。

NTILE()関数はBIGINT型のデータを返します。

**例:**

次の例では、パーティション内のすべての行を2つのバケットに分割します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```sql
select *,
    ntile(2)
        over (
            partition by subject
            order by score
        ) as bucket_id
from scores;
```

出力:

```plaintext
+------+-------+---------+-------+-----------+
| id   | name  | subject | score | bucket_id |
+------+-------+---------+-------+-----------+
|    6 | amber | NULL    |    90 |         1 |
|    1 | lily  | math    |  NULL |         1 |
|    5 | mike  | math    |    70 |         1 |
|    2 | tom   | math    |    80 |         1 |
|    4 | amy   | math    |    80 |         2 |
|    6 | amber | math    |    92 |         2 |
|    3 | jack  | math    |    95 |         2 |
|    3 | jack  | english |  NULL |         1 |
|    5 | mike  | english |    85 |         1 |
|    4 | amy   | english |    92 |         1 |
|    2 | tom   | english |    98 |         2 |
|    1 | lily  | english |   100 |         2 |
|    2 | tom   | physics |  NULL |         1 |
|    1 | lily  | physics |    60 |         1 |
|    5 | mike  | physics |    85 |         1 |
|    3 | jack  | physics |    99 |         2 |
|    4 | amy   | physics |    99 |         2 |
|    6 | amber | physics |   100 |         2 |
+------+-------+---------+-------+-----------+
```

上記の例のように、`num_buckets`が`2`の場合:

- 最初の行について、このパーティションにはこのレコードしかなく、1つのバケットに割り当てられます。
- 2行目から7行目について、このパーティションには6つのレコードがあり、最初の3つのレコードがバケット1に割り当てられ、他の3つのレコードがバケット2に割り当てられます。

### PERCENT_RANK()

結果セット内の行の相対ランクをパーセンテージとして計算します。

PERCENT_RANK()は次の式を使用して計算されます。ここで、`Rank`はパーティション内の現在の行のランクを表します。

```plaintext
(Rank - 1)/(Rows in partition - 1)
```

返される値は0から1の範囲です。この関数はパーセンタイル計算やデータ分布の分析に役立ちます。v3.2からサポートされています。

**構文:**

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

**この関数は、パーティション行を希望の順序にソートするためにORDER BYと一緒に使用する必要があります。**

**例:**

次の例では、`math`グループ内の各`score`の相対ランクを示します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
SELECT *,
    PERCENT_RANK()
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS `percent_rank`
FROM scores where subject in ('math');
```

```plaintext
+------+-------+---------+-------+--------------+
| id   | name  | subject | score | percent_rank |
+------+-------+---------+-------+--------------+
|    1 | lily  | math    |  NULL |            0 |
|    5 | mike  | math    |    70 |          0.2 |
|    2 | tom   | math    |    80 |          0.4 |
|    4 | amy   | math    |    80 |          0.4 |
|    6 | amber | math    |    92 |          0.8 |
|    3 | jack  | math    |    95 |            1 |
+------+-------+---------+-------+--------------+
```

### RANK()

RANK()関数はランキングを表現するために使用されます。DENSE_RANK()とは異なり、RANK()は**空白のある**番号が現れます。例えば、1が2つある場合、RANK()の3番目の番号は2ではなく3になります。

**構文:**

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

**例:**

グループ内のmathスコアをランク付けします。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *, 
    rank() over(
        partition by subject
        order by score desc
        ) as `rank`
from scores where subject in ('math');
```

```plain
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    4 | amy   | math    |    80 |    3 |
|    2 | tom   | math    |    80 |    3 |
|    5 | mike  | math    |    70 |    5 |
|    1 | lily  | math    |  NULL |    6 |
+------+-------+---------+-------+------+
```

結果データには、スコアが80の行が2つあります。これらはすべて3位です。次のスコア70のランクは5です。

### ROW_NUMBER()

パーティションの各行に対して1から始まる連続的に増加する整数を返します。RANK()やDENSE_RANK()とは異なり、ROW_NUMBER()が返す値は**繰り返しやギャップがなく**、**連続して増加します**。

**構文:**

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

**例:**

グループ内のmathスコアをランク付けします。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *, row_number() over(
    partition by subject
    order by score desc) as `rank`
from scores where subject in ('math');
```

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | rank |
+------+-------+---------+-------+------+
|    3 | jack  | math    |    95 |    1 |
|    6 | amber | math    |    92 |    2 |
|    2 | tom   | math    |    80 |    3 |
|    4 | amy   | math    |    80 |    4 |
|    5 | mike  | math    |    70 |    5 |
|    1 | lily  | math    |  NULL |    6 |
+------+-------+---------+-------+------+
```

### QUALIFY()

QUALIFY句はウィンドウ関数の結果をフィルタリングします。SELECT文で、QUALIFY句を使用して列に条件を適用し、結果をフィルタリングできます。QUALIFYは集計関数のHAVING句に類似しています。この関数はv2.5からサポートされています。

QUALIFYはSELECT文の記述を簡素化します。

QUALIFYが使用される前のSELECT文は次のようになります:

```SQL
SELECT *
FROM (SELECT DATE,
             PROVINCE_CODE,
             TOTAL_SCORE,
             ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) AS SCORE_ROWNUMBER
      FROM example_table) T1
WHERE T1.SCORE_ROWNUMBER = 1;
```

QUALIFYが使用された後、文は次のように短縮されます:

```SQL
SELECT DATE, PROVINCE_CODE, TOTAL_SCORE
FROM example_table 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) = 1;
```

QUALIFYは次の3つのウィンドウ関数のみをサポートします: ROW_NUMBER(), RANK(), および DENSE_RANK()。

**構文:**

```SQL
SELECT <column_list>
FROM <data_source>
[GROUP BY ...]
[HAVING ...]
QUALIFY <window_function>
[ ... ]
```

**パラメータ:**

`<column_list>`: データを取得したい列。

`<data_source>`: データソースは一般的にテーブルです。

`<window_function>`: `QUALIFY`句の後にはウィンドウ関数のみが続きます。ROW_NUMBER(), RANK(), および DENSE_RANK()を含みます。

**例:**

```SQL
-- テーブルを作成します。
CREATE TABLE sales_record (
   city_id INT,
   item STRING,
   sales INT
) DISTRIBUTED BY HASH(`city_id`);

-- テーブルにデータを挿入します。
insert into sales_record values
(1,'fruit',95),
(2,'drinks',70),
(3,'fruit',87),
(4,'drinks',98);

-- テーブルからデータをクエリします。
select * from sales_record order by city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       2 | drinks |    70 |
|       3 | fruit  |    87 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

例1: テーブルから行番号が1より大きいレコードを取得します。

```SQL
SELECT city_id, item, sales
FROM sales_record
QUALIFY row_number() OVER (ORDER BY city_id) > 1;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       2 | drinks |    70 |
|       3 | fruit  |    87 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

例2: テーブルの各パーティションから行番号が1のレコードを取得します。テーブルは`item`で2つのパーティションに分割され、各パーティションの最初の行が返されます。

```SQL
SELECT city_id, item, sales
FROM sales_record 
QUALIFY ROW_NUMBER() OVER (PARTITION BY item ORDER BY city_id) = 1
ORDER BY city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       2 | drinks |    70 |
+---------+--------+-------+
2 rows in set (0.01 sec)
```

例3: テーブルの各パーティションから売上が1位のレコードを取得します。テーブルは`item`で2つのパーティションに分割され、各パーティションで最も高い売上の行が返されます。

```SQL
SELECT city_id, item, sales
FROM sales_record
QUALIFY RANK() OVER (PARTITION BY item ORDER BY sales DESC) = 1
ORDER BY city_id;
+---------+--------+-------+
| city_id | item   | sales |
+---------+--------+-------+
|       1 | fruit  |    95 |
|       4 | drinks |    98 |
+---------+--------+-------+
```

**使用上の注意:**

- QUALIFYは次の3つのウィンドウ関数のみをサポートします: ROW_NUMBER(), RANK(), および DENSE_RANK()。

- QUALIFYを含むクエリの句の実行順序は次の順序で評価されます:

1. FROM
2. WHERE
3. GROUP BY
4. HAVING
5. Window
6. QUALIFY
7. DISTINCT
8. ORDER BY
9. LIMIT

### SUM()

指定された行の合計を計算します。

**構文:**

```SQL
SUM(expr) [OVER (analytic_clause)]
```

**例:**

`subject`でデータをグループ化し、グループ内のすべての行のスコアの合計を計算します。この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```SQL
select *,
    sum(score)
        over (
            partition by subject
            order by score
            rows between unbounded preceding and unbounded following
        ) as 'sum'
from scores;
```

```plaintext
+------+-------+---------+-------+------+
| id   | name  | subject | score | sum  |
+------+-------+---------+-------+------+
|    6 | amber | NULL    |    90 |   90 |
|    1 | lily  | math    |  NULL |  417 |
|    5 | mike  | math    |    70 |  417 |
|    2 | tom   | math    |    80 |  417 |
|    4 | amy   | math    |    80 |  417 |
|    6 | amber | math    |    92 |  417 |
|    3 | jack  | math    |    95 |  417 |
|    3 | jack  | english |  NULL |  375 |
|    5 | mike  | english |    85 |  375 |
|    4 | amy   | english |    92 |  375 |
|    2 | tom   | english |    98 |  375 |
|    1 | lily  | english |   100 |  375 |
|    2 | tom   | physics |  NULL |  443 |
|    1 | lily  | physics |    60 |  443 |
|    5 | mike  | physics |    85 |  443 |
|    3 | jack  | physics |    99 |  443 |
|    4 | amy   | physics |    99 |  443 |
|    6 | amber | physics |   100 |  443 |
+------+-------+---------+-------+------+
```

### VARIANCE, VAR_POP, VARIANCE_POP

式の母分散を返します。VAR_POPとVARIANCE_POPはVARIANCEのエイリアスです。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *,
    variance(score)
        over (
            partition by subject
            order by score
        ) as 'variance'
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | variance           |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 | 22.222222222222225 |
|    4 | amy   | math    |    80 | 22.222222222222225 |
|    6 | amber | math    |    92 |  60.74999999999997 |
|    3 | jack  | math    |    95 |  82.23999999999998 |
+------+-------+---------+-------+--------------------+
```

### VAR_SAMP, VARIANCE_SAMP

式の標本分散を返します。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
VAR_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *,
    VAR_SAMP(score)
       over (partition by subject
            order by score) as VAR_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | VAR_SAMP           |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 | 33.333333333333336 |
|    4 | amy   | math    |    80 | 33.333333333333336 |
|    6 | amber | math    |    92 |  80.99999999999996 |
|    3 | jack  | math    |    95 | 102.79999999999997 |
+------+-------+---------+-------+--------------------+
```

### STD, STDDEV, STDDEV_POP

式の標準偏差を返します。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
STD(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *, STD(score)
    over (
        partition by subject
        order by score) as std
from scores where subject in ('math');
+------+-------+---------+-------+-------------------+
| id   | name  | subject | score | std               |
+------+-------+---------+-------+-------------------+
|    1 | lily  | math    |  NULL |              NULL |
|    5 | mike  | math    |    70 |                 0 |
|    4 | amy   | math    |    80 | 4.714045207910317 |
|    2 | tom   | math    |    80 | 4.714045207910317 |
|    6 | amber | math    |    92 | 7.794228634059946 |
|    3 | jack  | math    |    95 | 9.068627239003707 |
+------+-------+---------+-------+-------------------+
```

### STDDEV_SAMP

式の標本標準偏差を返します。この関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
STDDEV_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *, STDDEV_SAMP(score)
    over (
        partition by subject
        order by score
        ) as STDDEV_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | STDDEV_SAMP        |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |               NULL |
|    5 | mike  | math    |    70 |                  0 |
|    2 | tom   | math    |    80 |  5.773502691896258 |
|    4 | amy   | math    |    80 |  5.773502691896258 |
|    6 | amber | math    |    92 |  8.999999999999998 |
|    3 | jack  | math    |    95 | 10.139033484509259 |
+------+-------+---------+-------+--------------------+

select *, STDDEV_SAMP(score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as STDDEV_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+--------------------+
| id   | name  | subject | score | STDDEV_SAMP        |
+------+-------+---------+-------+--------------------+
|    1 | lily  | math    |  NULL |                  0 |
|    5 | mike  | math    |    70 | 7.0710678118654755 |
|    2 | tom   | math    |    80 |  5.773502691896258 |
|    4 | amy   | math    |    80 |  8.999999999999998 |
|    6 | amber | math    |    92 | 10.139033484509259 |
|    3 | jack  | math    |    95 | 10.139033484509259 |
+------+-------+---------+-------+--------------------+
```

### COVAR_SAMP

2つの式の標本共分散を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
COVAR_SAMP(expr1,expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *, COVAR_SAMP(id, score) 
    over (
        partition by subject
        order by score) as covar_samp
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | covar_samp           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                 NULL |
|    5 | mike  | math    |    70 |                    0 |
|    2 | tom   | math    |    80 |   -6.666666666666668 |
|    4 | amy   | math    |    80 |   -6.666666666666668 |
|    6 | amber | math    |    92 |                  4.5 |
|    3 | jack  | math    |    95 | -0.24999999999999822 |
+------+-------+---------+-------+----------------------+

select *, COVAR_SAMP(id,score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as COVAR_SAMP
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | COVAR_SAMP           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                    0 |
|    5 | mike  | math    |    70 |                   -5 |
|    4 | amy   | math    |    80 |   -6.666666666666661 |
|    2 | tom   | math    |    80 |    4.500000000000004 |
|    6 | amber | math    |    92 | -0.24999999999999467 |
|    3 | jack  | math    |    95 | -0.24999999999999467 |
```

### COVAR_POP

2つの式の母共分散を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
COVAR_POP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *, COVAR_POP(id, score)
    over (
        partition by subject
        order by score) as covar_pop
from scores where subject in ('math');
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | covar_pop            |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL |                 NULL |
|    5 | mike  | math    |    70 |                    0 |
|    2 | tom   | math    |    80 |  -4.4444444444444455 |
|    4 | amy   | math    |    80 |  -4.4444444444444455 |
|    6 | amber | math    |    92 |                3.375 |
|    3 | jack  | math    |    95 | -0.19999999999999857 |
+------+-------+---------+-------+----------------------+
```

### CORR

2つの式間のピアソン相関係数を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートします。
:::

**パラメータ:**

`expr`がテーブルの列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

この例では、[サンプルテーブル](#window-function-sample-table) `scores`のデータを使用します。

```plaintext
select *, CORR(id, score)
    over (
        partition by subject
        order by score) as corr
from scores where subject in ('math');
+------+-------+---------+-------+-----------------------+
| id   | name  | subject | score | corr                  |
+------+-------+---------+-------+-----------------------+
|    5 | mike  | math    |    70 | -0.015594571538795355 |
|    1 | lily  | math    |  NULL | -0.015594571538795355 |
|    2 | tom   | math    |    80 | -0.015594571538795355 |
|    4 | amy   | math    |    80 | -0.015594571538795355 |
|    3 | jack  | math    |    95 | -0.015594571538795355 |
|    6 | amber | math    |    92 | -0.015594571538795355 |
+------+-------+---------+-------+-----------------------+

```plaintext
select *, CORR(id,score)
    over (
        partition by subject
        order by score
        rows between unbounded preceding and 1 following) as corr 
from scores where subject in ('math');
+------+-------+---------+-------+-------------------------+
| id   | name  | subject | score | corr                    |
+------+-------+---------+-------+-------------------------+
|    1 | lily  | math    |  NULL | 1.7976931348623157e+308 |
|    5 | mike  | math    |    70 |                      -1 |
|    2 | tom   | math    |    80 |     -0.7559289460184546 |
|    4 | amy   | math    |    80 |     0.29277002188455997 |
|    6 | amber | math    |    92 |   -0.015594571538795024 |
|    3 | jack  | math    |    95 |   -0.015594571538795024 |
+------+-------+---------+-------+-------------------------+
```