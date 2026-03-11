---
displayed_sidebar: docs
keywords: ['ウィンドウ関数', 'ウィンドウ']
sidebar_position: 0.9
---

# ウィンドウ関数

## 背景

ウィンドウ関数は、組み込み関数の特殊なクラスです。集計関数と同様に、複数の入力行に対して計算を行い、単一のデータ値を取得します。違いは、ウィンドウ関数が「group by」メソッドを使用するのではなく、特定のウィンドウ内で入力データを処理することです。各ウィンドウ内のデータは、over()句を使用してソートおよびグループ化できます。ウィンドウ関数は**各行に対して個別の値を計算します**、各グループに対して1つの値を計算するのではなく、この柔軟性により、ユーザーはselect句に追加の列を追加し、結果セットをさらにフィルタリングできます。ウィンドウ関数は、selectリストと句の最も外側の位置にのみ表示できます。クエリの最後に、つまり`join`、`where`、および`group by`の操作が実行された後に効果を発揮します。ウィンドウ関数は、トレンドの分析、外れ値の計算、大規模データに対するバケット分析によく使用されます。

## 使用法

### 構文

```SQL
function(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### PARTITION BY句

Partition By句はGroup Byに似ています。1つ以上の指定された列によって入力行をグループ化します。同じ値を持つ行はグループ化されます。

### ORDER BY句

`Order By`句は、基本的に外側の`Order By`と同じです。入力行の順序を定義します。`Partition By`が指定されている場合、`Order By`は各パーティショングループ内の順序を定義します。唯一の違いは、`OVER`句の`Order By n`（nは正の整数）が操作なしと同等であるのに対し、外側の`Order By`の`n`はn番目の列によるソートを示すことです。

例:

この例では、eventsテーブルの`date_and_time`列でソートされた、1、2、3などの値を持つid列をselectリストに追加する方法を示します。

```SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
```

### ウィンドウ句

ウィンドウ句は、操作の行範囲（現在の行に基づく先行および後続の行）を指定するために使用されます。以下の構文をサポートしています: AVG()、COUNT()、FIRST_VALUE()、LAST_VALUE()、およびSUM()。MAX()とMIN()の場合、ウィンドウ句は開始から`UNBOUNDED PRECEDING`までを指定できます。

**構文:**

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
RANGE BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

:::note
**ARRAY_AGG() ウィンドウフレームの制限:**

ARRAY_AGG()をウィンドウ関数として使用する場合、RANGEフレームのみがサポートされます。ROWSフレームはサポートされません。例:

```SQL
-- サポート対象: RANGEフレーム
array_agg(col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- サポート対象外: ROWSフレーム (エラーが発生します)
array_agg(col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```

:::

## ウィンドウ関数サンプルテーブル

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

## 関数例

このセクションでは、StarRocksでサポートされているウィンドウ関数について説明します。

### AVG()

指定されたウィンドウ内のフィールドの平均値を計算します。この関数はNULL値を無視します。

**構文:**

```SQL
AVG([DISTINCT] expr) [OVER (*analytic_clause*)]
```

`DISTINCT`はStarRocks v4.0からサポートされています。指定すると、AVG()はウィンドウ内の異なる値のみの平均を計算します。

:::note
**ウィンドウフレームの制限:**

AVG(DISTINCT)をウィンドウ関数として使用する場合、RANGEフレームのみがサポートされます。ROWSフレームはサポートされません。
:::

**例**

**例1: 基本的な使用法**

次の例では、株価データを例として使用します。

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

現在の行と、その前後の各行の平均終値を計算します。

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

例えば、最初の行の`12.87500000`は、「2014-10-02」（`12.86`）、その前日「2014-10-01」（null）、およびその翌日「2014-10-03」（`12.89`）の終値の平均値です。

**例2: 全体ウィンドウでのAVG(DISTINCT)の使用**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

すべての行で重複しないスコアの平均を計算します。

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER () AS distinct_avg
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_avg|
+----+---------+-------+-------------+
|  1 | math    |    80 |       85.00 |
|  2 | math    |    85 |       85.00 |
|  3 | math    |    80 |       85.00 |
|  4 | english |    90 |       85.00 |
|  5 | english |    85 |       85.00 |
|  6 | english |    90 |       85.00 |
+----+---------+-------+-------------+
```

重複しない平均は85.00です (`(80 + 85 + 90) / 3`)。

**例3: RANGEフレームを持つフレーム付きウィンドウでAVG(DISTINCT)を使用する**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

RANGEフレームを使用して、各科目パーティション内で重複しないスコアの平均を計算します。

```SQL
SELECT id, subject, score,
    AVG(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_avg
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_avg|
+----+---------+-------+-------------+
|  1 | math    |    80 |       80.00 |
|  3 | math    |    80 |       80.00 |
|  2 | math    |    85 |       82.50 |
|  5 | english |    85 |       85.00 |
|  4 | english |    90 |       87.50 |
|  6 | english |    90 |       87.50 |
+----+---------+-------+-------------+
```

各行について、この関数はパーティションの開始から現在の行のスコア値まで（現在の行のスコア値を含む）の重複しないスコアの平均を計算します。

### ARRAY_AGG()

ウィンドウ内の値（NULL値を含む）を配列に集約します。オプションの `ORDER BY` 句を使用して、配列内の要素をソートできます。

この関数はv3.4以降でサポートされています。

:::tip
**ウィンドウフレームの制限:**

ウィンドウ関数としての ARRAY_AGG() は RANGE ウィンドウフレームのみをサポートします。ROWS ウィンドウフレームはサポートされていません。ウィンドウフレームが指定されていない場合、デフォルトの `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` が使用されます。
:::

**構文:**

```SQL
ARRAY_AGG([DISTINCT] expr [ORDER BY expr [ASC | DESC]]) OVER([partition_by_clause] [order_by_clause] [window_clause])
```

**パラメータ:**

-  `expr`: 集約する式。サポートされている任意のデータ型の列にすることができます。
-  `DISTINCT`: オプション。結果配列から重複する値を削除します。
-  `ORDER BY`: オプション。配列内の要素の順序を指定します。

**戻り値:**

ウィンドウ内のすべての値を含むARRAYを返します。

**使用上の注意:**

- **ROWSフレームはサポートされていません。** ウィンドウ関数としてのARRAY_AGG()では、RANGEフレームのみを使用できます。ROWSフレームを使用するとエラーになります。
- NULL値は結果配列に含まれます。
-  `DISTINCT` が指定されている場合、重複する値は配列から削除されます。
- ARRAY_AGG()内で `ORDER BY` が指定されている場合、結果の配列内の要素はそれに応じてソートされます。

**例**

これらの例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

**例1: 基本的な使用法**

各科目パーティション内のすべてのスコアを収集します。

```SQL
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS score_array
FROM scores
WHERE subject = 'math';
```

出力:

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | score_array          |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [null]               |
|    5 | mike  | math    |    70 | [null,70]            |
|    2 | tom   | math    |    80 | [null,70,80,80]      |
|    4 | amy   | math    |    80 | [null,70,80,80]      |
|    6 | amber | math    |    92 | [null,70,80,80,92]   |
|    3 | jack  | math    |    95 | [null,70,80,80,92,95]|
+------+-------+---------+-------+----------------------+
```

注: 同じ `score` 値を持つ行（トムとエイミーは両方とも80）は、RANGEフレームのセマンティクスにより同じ配列を受け取ります。

**例2: ウィンドウでの ARRAY_AGG(DISTINCT)**

各科目パーティション内で重複しないスコアを収集します。

```SQL
SELECT *,
    array_agg(DISTINCT score)
        OVER (
            PARTITION BY subject
            ORDER BY score
        ) AS distinct_scores
FROM scores
WHERE subject = 'math';
```

出力:

```plaintext
+------+-------+---------+-------+-------------------+
| id   | name  | subject | score | distinct_scores   |
+------+-------+---------+-------+-------------------+
|    1 | lily  | math    |  NULL | [null]            |
|    5 | mike  | math    |    70 | [null,70]         |
|    2 | tom   | math    |    80 | [null,70,80]      |
|    4 | amy   | math    |    80 | [null,70,80]      |
|    6 | amber | math    |    92 | [null,70,80,92]   |
|    3 | jack  | math    |    95 | [null,70,80,92,95]|
+------+-------+---------+-------+-------------------+
```

**例3: ORDER BY 句を使用した ARRAY_AGG()**

配列内でスコアを降順にソートして収集します。

```SQL
SELECT *,
    array_agg(score ORDER BY score DESC)
        OVER (
            PARTITION BY subject
        ) AS scores_desc
FROM scores
WHERE subject = 'math';
```

出力:

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | scores_desc          |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [95,92,80,80,70,null]|
|    5 | mike  | math    |    70 | [95,92,80,80,70,null]|
|    2 | tom   | math    |    80 | [95,92,80,80,70,null]|
|    4 | amy   | math    |    80 | [95,92,80,80,70,null]|
|    6 | amber | math    |    92 | [95,92,80,80,70,null]|
|    3 | jack  | math    |    95 | [95,92,80,80,70,null]|
+------+-------+---------+-------+----------------------+
```

**例4: RANGEフレームを使用したARRAY_AGG()**

RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWINGを使用して、パーティション全体のすべてのスコアを収集します。

```SQL
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
            RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS all_scores
FROM scores
WHERE subject = 'math';
```

出力:

```plaintext
+------+-------+---------+-------+----------------------+
| id   | name  | subject | score | all_scores           |
+------+-------+---------+-------+----------------------+
|    1 | lily  | math    |  NULL | [null,70,80,80,92,95]|
|    5 | mike  | math    |    70 | [null,70,80,80,92,95]|
|    2 | tom   | math    |    80 | [null,70,80,80,92,95]|
|    4 | amy   | math    |    80 | [null,70,80,80,92,95]|
|    6 | amber | math    |    92 | [null,70,80,80,92,95]|
|    3 | jack  | math    |    95 | [null,70,80,80,92,95]|
+------+-------+---------+-------+----------------------+
```

**例5: スコア範囲でパーティション化された名前を収集する**

`stock_ticker`テーブルを使用して、移動ウィンドウで株価記号を収集します。

```SQL
SELECT
    stock_symbol,
    closing_date,
    closing_price,
    array_agg(closing_price)
        OVER (
            PARTITION BY stock_symbol
            ORDER BY closing_date
        ) AS price_history
FROM stock_ticker;
```

出力:

```plaintext
+--------------+---------------------+---------------+---------------------------------------+
| stock_symbol | closing_date        | closing_price | price_history                         |
+--------------+---------------------+---------------+---------------------------------------+
| JDR          | 2014-10-02 00:00:00 |         12.86 | [12.86]                               |
| JDR          | 2014-10-03 00:00:00 |         12.89 | [12.86,12.89]                         |
| JDR          | 2014-10-04 00:00:00 |         12.94 | [12.86,12.89,12.94]                   |
| JDR          | 2014-10-05 00:00:00 |         12.55 | [12.86,12.89,12.94,12.55]             |
| JDR          | 2014-10-06 00:00:00 |         14.03 | [12.86,12.89,12.94,12.55,14.03]       |
| JDR          | 2014-10-07 00:00:00 |         14.75 | [12.86,12.89,12.94,12.55,14.03,14.75] |
| JDR          | 2014-10-08 00:00:00 |         13.98 | [12.86,12.89,12.94,12.55,14.03,14.75,13.98] |
+--------------+---------------------+---------------+---------------------------------------+
```

**例6: 無効な使用法 - ROWSフレーム (エラーが発生します)**

ROWSフレームはサポートされていないため、以下のクエリは失敗します。

```SQL
-- これによりエラーが発生します！
SELECT *,
    array_agg(score)
        OVER (
            PARTITION BY subject
            ORDER BY score
            ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING  -- NOT SUPPORTED!
        ) AS score_array
FROM scores;
```

エラーメッセージ:

```plaintext
ERROR: array_agg as window function does not support ROWS frame type. Please use RANGE frame instead.
```

### COUNT()

指定されたウィンドウで、指定された条件を満たす行の総数を計算します。

**構文:**

```SQL
COUNT([DISTINCT] expr) [OVER (analytic_clause)]
```

`DISTINCT`はStarRocks v4.0からサポートされています。指定すると、COUNT()はウィンドウ内の重複しない値のみをカウントします。

:::note
**ウィンドウフレームの制限:**

ウィンドウ関数としてCOUNT(DISTINCT)を使用する場合、RANGEフレームのみがサポートされます。ROWSフレームはサポートされていません。例:

```SQL
-- サポート対象: RANGEフレーム
count(distinct col) OVER (PARTITION BY x ORDER BY y RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- サポート対象外: ROWSフレーム (エラーが発生します)
count(distinct col) OVER (PARTITION BY x ORDER BY y ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
```

:::

**例**

これらの例では、[示例表](#ウィンドウ関数サンプルテーブル)`scores`のデータを使用します。

**例1: 基本的な使用法**

現在の行から数学パーティションの最初の行まで、90より大きい数学のスコアの出現回数をカウントします。この例では、[示例表](#ウィンドウ関数サンプルテーブル)`scores`のデータを使用します。

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

**例2: 全体ウィンドウでのCOUNT(DISTINCT)の使用**

すべての行で重複しないスコアをカウントします。

```SQL
CREATE TABLE test_scores (
    id INT,
    subject VARCHAR(20),
    score INT
) DISTRIBUTED BY HASH(id);

INSERT INTO test_scores VALUES
    (1, 'math', 80),
    (2, 'math', 85),
    (3, 'math', 80),
    (4, 'english', 90),
    (5, 'english', 85),
    (6, 'english', 90);
```

```SQL
SELECT id, subject, score,
    COUNT(DISTINCT score) OVER () AS distinct_count
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+---------------+
| id | subject | score | distinct_count|
+----+---------+-------+---------------+
|  1 | math    |    80 |             4 |
|  2 | math    |    85 |             4 |
|  3 | math    |    80 |             4 |
|  4 | english |    90 |             4 |
|  5 | english |    85 |             4 |
|  6 | english |    90 |             4 |
+----+---------+-------+---------------+
```

重複しないカウントは4です (値: 80, 85, 90, およびNULLがあればそれら)。

**例3: RANGEフレームを使用したフレーム付きウィンドウでのCOUNT(DISTINCT)の使用**

RANGEフレームを使用して、各科目パーティション内で重複しないスコアをカウントします。

```SQL
SELECT id, subject, score,
    COUNT(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_count
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+---------------+
| id | subject | score | distinct_count|
+----+---------+-------+---------------+
|  1 | math    |    80 |             1 |
|  3 | math    |    80 |             1 |
|  2 | math    |    85 |             2 |
|  5 | english |    85 |             1 |
|  4 | english |    90 |             2 |
|  6 | english |    90 |             2 |
+----+---------+-------+---------------+
```

各行について、関数はパーティションの最初から現在の行のスコア値まで（現在の行のスコア値を含む）の重複しないスコアをカウントします。

### CUME_DIST()

CUME_DIST()関数は、パーティションまたはウィンドウ内の値の累積分布を計算し、パーティション内での相対的な位置をパーセンテージで示します。これは、グループ内の最高値または最低値の分布を計算するためによく使用されます。

- データが昇順にソートされている場合、この関数は現在の行の値以下の値のパーセンテージを計算します。
- データが降順にソートされている場合、この関数は現在の行の値以上の値のパーセンテージを計算します。

累積分布は0から1の範囲です。これは、パーセンタイル計算やデータ分布分析に役立ちます。

この関数はv3.2以降でサポートされています。

**構文:**

```SQL
CUME_DIST() OVER (partition_by_clause order_by_clause)
```

- `partition_by_clause`: オプション。この句が指定されていない場合、結果セット全体が単一のパーティションとして処理されます。
- `order_by_clause`: **この関数は、パーティションの行を目的の順序にソートするためにORDER BYと一緒に使用する必要があります。**

CUME_DIST()はNULL値を含み、それらを最低値として扱います。

**例**

次の例は、各`subject`グループ内の各スコアの累積分布を示しています。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

- 最初の行の`cume_dist`の場合、`NULL`グループには1行しかなく、この行自体のみが「現在の行以下」という条件を満たします。累積分布は1です。
- 2番目の行の`cume_dist`の場合、`english`グループには5行があり、この行自体（NULL）のみが「現在の行以下」という条件を満たします。累積分布は0.2です。
- 3番目の行の`cume_dist`の場合、`english`グループには5行があり、2行（85とNULL）が「現在の行以下」という条件を満たします。累積分布は0.4です。

### DENSE_RANK()

DENSE_RANK()関数はランキングを表すために使用されます。RANK()とは異なり、DENSE_RANK()は**空きがありません** 番号です。例えば、1が2つある場合、DENSE_RANK()の3番目の番号は2のままであり、RANK()の3番目の番号は3です。

**構文:**

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

**例**

次の例は、数学のスコアのランキング（降順にソート）を示しています。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

結果データにはスコアが80の行が2つあります。これらはすべてランク3です。次のスコア70のランクは4です。これはDENSE_RANK()が**空きがありません** 番号です。

### FIRST_VALUE()

FIRST_VALUE()は**最初の** ウィンドウ範囲の値を返します。

**構文:**

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0以降でサポートされています。これは、`expr`のNULL値が計算から除外されるかどうかを決定するために使用されます。デフォルトでは、NULL値は含まれます。つまり、フィルタリングされた結果の最初の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定した場合、フィルタリングされた結果の最初の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

ARRAY型はStarRocks v3.5以降でサポートされています。FIRST_VALUE()をARRAY列で使用して、ウィンドウ内の最初の配列値を取得できます。

**例**

**例1: 基本的な使用法**

各グループの各メンバーについて、`subject`でグループ化し、最初の`score`値を返します（降順）。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

**例 2: ARRAY型でFIRST_VALUE()を使用する**

ARRAY列を持つテーブルを作成する:

```SQL
CREATE TABLE test_array_value (
    col_1 INT,
    arr1 ARRAY<INT>
) DISTRIBUTED BY HASH(col_1);

INSERT INTO test_array_value (col_1, arr1) VALUES
    (1, [1, 11]),
    (2, [2, 22]),
    (3, [3, 33]),
    (4, NULL),
    (5, [5, 55]);
```

ARRAY型でFIRST_VALUE()を使用してデータをクエリする:

```SQL
SELECT col_1, arr1, 
    FIRST_VALUE(arr1) OVER (ORDER BY col_1) AS first_array
FROM test_array_value;
```

出力:

```plaintext
+-------+--------+------------+
| col_1 | arr1   | first_array|
+-------+--------+------------+
|     1 | [1,11] | [1,11]     |
|     2 | [2,22] | [1,11]     |
|     3 | [3,33] | [1,11]     |
|     4 | NULL   | [1,11]     |
|     5 | [5,55] | [1,11]     |
+-------+--------+------------+
```

ウィンドウ内のすべての行に対して、最初の配列値`[1,11]`が返されます。

### LAST_VALUE()

LAST_VALUE()は**最後の**ウィンドウ範囲の値を返します。FIRST_VALUE()の反対です。

**構文:**

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0からサポートされています。これは、`expr`のNULL値が計算から除外されるかどうかを決定するために使用されます。デフォルトでは、NULL値は含まれ、フィルタリングされた結果の最後の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定すると、フィルタリングされた結果の最後の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

デフォルトでは、LAST_VALUE()は`rows between unbounded preceding and current row`を計算します。これは現在の行とその前のすべての行を比較します。各パーティションに1つの値のみを表示したい場合は、ORDER BYの後に`rows between unbounded preceding and unbounded following`を使用します。

ARRAY型はStarRocks v3.5からサポートされています。LAST_VALUE()をARRAY列で使用して、ウィンドウ内の最後の配列値を取得できます。

**例**

**例 1: 基本的な使用法**

グループ内の各メンバーの最後の`score`を返します（降順）。`subject`でグループ化します。この例では、[示例表](#ウィンドウ関数サンプルテーブル)`scores`のデータを使用します。

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

**例 2: ARRAY型でLAST_VALUE()を使用する**

FIRST_VALUE()の例 2と同じテーブルを使用します:

```SQL
SELECT col_1, arr1, 
    LAST_VALUE(arr1) OVER (
        ORDER BY col_1 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_array
FROM test_array_value;
```

出力:

```plaintext
+-------+--------+-----------+
| col_1 | arr1   | last_array|
+-------+--------+-----------+
|     1 | [1,11] | [5,55]    |
|     2 | [2,22] | [5,55]    |
|     3 | [3,33] | [5,55]    |
|     4 | NULL   | [5,55]    |
|     5 | [5,55] | [5,55]    |
+-------+--------+-----------+
```

ウィンドウ内のすべての行に対して、最後の配列値`[5,55]`が返されます。

### LAG()

現在の行から`offset`行遅れている行の値を返します。この関数は、行間の値を比較したり、データをフィルタリングしたりするためによく使用されます。

`LAG()`は、以下の型のデータをクエリするために使用できます:

- 数値型: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
- 文字列型: CHAR, VARCHAR
- 日付型: DATE, DATETIME
- BITMAPとHLLはStarRocks v2.5からサポートされています。
- ARRAY型はStarRocks v3.5からサポートされています。

**構文:**

```SQL
LAG(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**パラメータ:**

- `expr`: 計算したいフィールド。
- `offset`: オフセット。これは**正の整数**である必要があります。このパラメータが指定されていない場合、デフォルトは1です。
- `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されていない場合、デフォルトはNULLです。`default`は、バージョン4.0以降、`expr`と互換性のある型の任意の式をサポートしており、デフォルトは定数である必要はなく、列名にすることもできます。
- `IGNORE NULLS`はv3.0からサポートされています。これは、`expr`のNULL値が結果に含まれるかどうかを決定するために使用されます。デフォルトでは、`offset`行がカウントされるときにNULL値が含まれ、これは対象行の値がNULLの場合にNULLが返されることを意味します。例1を参照してください。IGNORE NULLSを指定すると、`offset`行がカウントされるときにNULL値は無視され、システムは`offset`個の非NULL値を検索し続けます。`offset`個の非NULL値が見つからない場合、NULLまたは`default`（指定されている場合）が返されます。例2を参照してください。

**例**

**例 1: IGNORE NULLS が指定されていない場合**

テーブルを作成し、値を挿入します。

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

このテーブルからデータをクエリします。ここで、`offset` は 2 で、前の2行を走査することを意味します。`default` は 0 で、一致する行が見つからない場合に 0 が返されることを意味します。

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

最初の2行については、前の2行が存在しないため、デフォルト値の 0 が返されます。

3行目の NULL については、2行前の値が NULL であり、NULL 値が許可されているため NULL が返されます。

**例 2: IGNORE NULLS が指定されている場合**

上記のテーブルとパラメータ設定を使用します。

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

1行目から4行目については、システムは前の行にそれぞれ2つの非NULL値を見つけることができず、デフォルト値の 0 が返されます。

7行目の値 6 については、2行前の値が NULL であり、IGNORE NULLS が指定されているため NULL は無視されます。システムは非NULL値の検索を続け、4行目の 2 が返されます。

**例 3: LAG() のデフォルト値を列名に設定する**

上記のテーブルとパラメータ設定を使用します。

```SQL
SELECT col_1, col_2, LAG(col_2 ,2,col_1) OVER (ORDER BY col_1)
FROM test_tbl ORDER BY col_1;
+-------+-------+-------------------------------------------------+
| col_1 | col_2 | lag(col_2, 2, col_1) OVER (ORDER BY col_1 ASC ) |
+-------+-------+-------------------------------------------------+
|     1 |  NULL |                                               1 |
|     2 |     4 |                                               2 |
|     3 |  NULL |                                            NULL |
|     4 |     2 |                                               4 |
|     5 |  NULL |                                            NULL |
|     6 |     7 |                                               2 |
|     7 |     6 |                                            NULL |
|     8 |     5 |                                               7 |
|     9 |  NULL |                                               6 |
|    10 |  NULL |                                               5 |
+-------+-------+-------------------------------------------------+
```

ご覧のとおり、1行目と2行目については、後方にスキャンしても2つの非NULL値がないため、デフォルトで返されるのは現在の行の col_1 の値です。

その他のすべての行は、例 1 と同じように動作します。

**例 4: ARRAY 型で LAG() を使用する**

ARRAY 列を持つテーブルを作成します。

```SQL
CREATE TABLE test_array_value (
    col_1 INT,
    arr1 ARRAY<INT>,
    arr2 ARRAY<INT> NOT NULL
) DISTRIBUTED BY HASH(col_1);

INSERT INTO test_array_value (col_1, arr1, arr2) VALUES
    (1, [1, 11], [101, 111]),
    (2, [2, 22], [102, 112]),
    (3, [3, 33], [103, 113]),
    (4, NULL,    [104, 114]),
    (5, [5, 55], [105, 115]),
    (6, [6, 66], [106, 116]);
```

ARRAY 型で LAG() を使用してデータをクエリします。

```SQL
SELECT col_1, arr1, LAG(arr1, 2, arr2) OVER (ORDER BY col_1) AS lag_result 
FROM test_array_value;
```

出力:

```plaintext
+-------+--------+-------------+
| col_1 | arr1   | lag_result  |
+-------+--------+-------------+
|     1 | [1,11] | [101,111]   |
|     2 | [2,22] | [102,112]   |
|     3 | [3,33] | [1,11]      |
|     4 | NULL   | [2,22]      |
|     5 | [5,55] | [3,33]      |
|     6 | [6,66] | NULL        |
+-------+--------+-------------+
```

最初の2行については、前の2行が存在しないため、`arr2` からのデフォルト値が返されます。

### LEAD()

現在の行から `offset` 行先の行の値を返します。この関数は、行間の値を比較したり、データをフィルタリングしたりするためによく使用されます。

`LEAD()` でクエリできるデータ型は、[LAG()](#lag)でサポートされているものと同じです。

**構文:**

```sql
LEAD(expr [IGNORE NULLS] [, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

**パラメータ:**

- `expr`: 計算したいフィールド。
- `offset`: オフセット。正の整数である必要があります。このパラメータが指定されていない場合、デフォルトは 1 です。
- `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されていない場合、デフォルトは NULL です。`default` は、バージョン 4.0 以降、`expr` と互換性のある型の任意の式をサポートしており、デフォルトは定数である必要はなく、列名にすることができます。
- `IGNORE NULLS` は v3.0 からサポートされています。これは、`expr` の NULL 値が結果に含まれるかどうかを決定するために使用されます。デフォルトでは、`offset` 行がカウントされるときに NULL 値が含まれます。これは、対象行の値が NULL の場合に NULL が返されることを意味します。例 1 を参照してください。IGNORE NULLS を指定すると、`offset` 行がカウントされるときに NULL 値は無視され、システムは `offset` の非NULL値を検索し続けます。`offset` の非NULL値が見つからない場合、NULL または `default` (指定されている場合) が返されます。例 2 を参照してください。

**例**

**例 1: IGNORE NULLS が指定されていない場合**

テーブルを作成し、値を挿入します。

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

このテーブルからデータをクエリします。ここで、`offset` は 2 で、後続の2行を走査することを意味します。`default` は 0 で、一致する行が見つからない場合に 0 が返されることを意味します。

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

最初の行については、2行先の値が NULL であり、NULL 値が許可されているため NULL が返されます。

最後の2行については、後続の2行が存在しないため、デフォルト値の 0 が返されます。

**例 2: IGNORE NULLS が指定されている場合**

上記のテーブルとパラメータ設定を使用します。

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

7行目から10行目については、システムは後続の行に2つの非NULL値を見つけることができず、デフォルト値の 0 が返されます。

最初の行の場合、2行先の値はNULLであり、IGNORE NULLSが指定されているためNULLは無視されます。システムは2番目の非NULL値を検索し続け、4行目の2が返されます。

**例3: LEAD()のデフォルト値を列名に設定する**

前のテーブルとパラメータ設定を使用します。

```SQL
SELECT col_1, col_2, LEAD(col_2 ,2,col_1) OVER (ORDER BY col_1)
FROM test_tbl ORDER BY col_1;
+-------+-------+--------------------------------------------------+
| col_1 | col_2 | lead(col_2, 2, col_1) OVER (ORDER BY col_1 ASC ) |
+-------+-------+--------------------------------------------------+
|     1 |  NULL |                                             NULL |
|     2 |     4 |                                                2 |
|     3 |  NULL |                                             NULL |
|     4 |     2 |                                                7 |
|     5 |  NULL |                                                6 |
|     6 |     7 |                                                5 |
|     7 |     6 |                                             NULL |
|     8 |     5 |                                             NULL |
|     9 |  NULL |                                                9 |
|    10 |  NULL |                                               10 |
+-------+-------+--------------------------------------------------+
```

ご覧のとおり、9行目と10行目では、前方スキャン時に2つの非NULL値がないため、返されるデフォルト値は現在の行のcol_1値になります。

その他のすべての行は、例1と同じように動作します。

**例4: ARRAY型でLEAD()を使用する**

LAG()の例4と同じテーブルを使用します。

```SQL
SELECT col_1, arr1, LEAD(arr1, 2, arr2) OVER (ORDER BY col_1) AS lead_result 
FROM test_array_value;
```

出力:

```plaintext
+-------+--------+-------------+
| col_1 | arr1   | lead_result |
+-------+--------+-------------+
|     1 | [1,11] | [3,33]      |
|     2 | [2,22] | NULL        |
|     3 | [3,33] | [5,55]      |
|     4 | NULL   | [6,66]      |
|     5 | [5,55] | [105,115]   |
|     6 | [6,66] | [106,116]   |
+-------+--------+-------------+
```

最後の2行については、後続の2行が存在しないため、`arr2`からのデフォルト値が返されます。

### MAX()

現在のウィンドウで指定された行の最大値を返します。

**構文:**

```SQL
MAX(expr) [OVER (analytic_clause)]
```

**例**

最初の行から現在の行の次の行までの行の最大値を計算します。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

StarRocks 2.4以降では、行範囲を`rows between n preceding and n following`として指定できます。これは、現在の行の前の`n`行と、現在の行の後の`n`行をキャプチャできることを意味します。

ステートメントの例:

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

現在のウィンドウで指定された行の最小値を返します。

**構文:**

```SQL
MIN(expr) [OVER (analytic_clause)]
```

**例**

数学の科目のすべての行の中で最低スコアを計算します。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

StarRocks 2.4以降では、行範囲を`rows between n preceding and n following`として指定できます。これは、現在の行の前の`n`行と、現在の行の後の`n`行をキャプチャできることを意味します。

ステートメントの例:

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

NTILE()関数は、パーティション内のソートされた行を指定された`num_buckets`の数で可能な限り均等に分割し、分割された行を1`[1, 2, ..., num_buckets]`から始まるそれぞれのバケットに格納し、各行が属するバケット番号を返します。

バケットのサイズについて:

- 行数が指定された`num_buckets`の数で正確に割り切れる場合、すべてのバケットは同じサイズになります。
- 行数が指定された`num_buckets`の数で正確に割り切れない場合、2つの異なるサイズのバケットが存在します。サイズの差は1です。行数が多いバケットは、行数が少ないバケットよりも先にリストされます。

**構文:**

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`: 作成するバケットの数。値は、最大値が`2^63 - 1`である定数の正の整数である必要があります。

NTILE()関数ではウィンドウ句は許可されません。

NTILE()関数はBIGINT型のデータを返します。

**例**

次の例では、パーティション内のすべての行を2つのバケットに分割します。この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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

上記の例が示すように、`num_buckets` が `2` の場合:

- 最初の行の場合、このパーティションにはこのレコードのみがあり、1つのバケットにのみ割り当てられます。
- 2行目から7行目まで、パーティションには6つのレコードがあり、最初の3つのレコードはバケット1に割り当てられ、他の3つのレコードはバケット2に割り当てられます。

### PERCENT_RANK()

結果セット内の行の相対的なランクをパーセンテージとして計算します。

PERCENT_RANK() は次の式を使用して計算されます。ここで、`Rank` はパーティション内の現在の行のランクを表します。

```plaintext
(Rank - 1)/(Rows in partition - 1)
```

戻り値の範囲は0から1です。この関数は、パーセンタイル計算やデータ分布の分析に役立ちます。v3.2からサポートされています。

**構文:**

```SQL
PERCENT_RANK() OVER (partition_by_clause order_by_clause)
```

:::note
PERCENT_RANK() は、パーティションの行を目的の順序に並べ替えるために ORDER BY と一緒に使用する必要があります。
:::

**例**

次の例は、`math` のグループ内における各 `score` の相対的なランクを示しています。この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用しています。

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

RANK() 関数はランキングを表すために使用されます。DENSE_RANK() とは異なり、RANK() は**空きとして表示されます**番号になります。例えば、2つの同順位の1が出現した場合、RANK() の3番目の番号は2ではなく3になります。

**構文:**

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

**例**

グループ内の数学のスコアをランク付けします。この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用しています。

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

結果データにはスコアが80の行が2つあります。これらはすべてランク3です。次のスコア70のランクは5です。

### ROW_NUMBER()

パーティションの各行に対して1から始まる連続的に増加する整数を返します。RANK() や DENSE_RANK() とは異なり、ROW_NUMBER() が返す値は**重複したり、ギャップがあったりしません**であり、**連続的に増加します**です。

**構文:**

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

**例**

グループ内の数学のスコアをランク付けします。この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用しています。

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

QUALIFY句はウィンドウ関数の結果をフィルタリングします。SELECT文では、QUALIFY句を使用して列に条件を適用し、結果をフィルタリングできます。QUALIFYは集計関数におけるHAVING句に類似しています。この関数はv2.5からサポートされています。

QUALIFYはSELECT文の記述を簡素化します。

QUALIFYを使用する前は、SELECT文は次のようになる場合があります:

```SQL
SELECT *
FROM (SELECT DATE,
             PROVINCE_CODE,
             TOTAL_SCORE,
             ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) AS SCORE_ROWNUMBER
      FROM example_table) T1
WHERE T1.SCORE_ROWNUMBER = 1;
```

QUALIFYを使用すると、ステートメントは次のように短縮されます。

```SQL
SELECT DATE, PROVINCE_CODE, TOTAL_SCORE
FROM example_table 
QUALIFY ROW_NUMBER() OVER(PARTITION BY PROVINCE_CODE ORDER BY TOTAL_SCORE) = 1;
```

QUALIFYは、ROW_NUMBER()、RANK()、DENSE_RANK()の3つのウィンドウ関数のみをサポートしています。

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

`<data_source>`: データソースは通常テーブルです。

`<window_function>`: `QUALIFY`句の後には、ROW_NUMBER()、RANK()、DENSE_RANK()を含むウィンドウ関数のみが続きます。

**例**

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

例2: テーブルの各パーティションから行番号が1のレコードを取得します。テーブルは`item`によって2つのパーティションに分割され、各パーティションの最初の行が返されます。

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

例3: テーブルの各パーティションから売上ランク1位のレコードを取得します。テーブルは`item`によって2つのパーティションに分割され、各パーティションで最も売上が高い行が返されます。

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

- QUALIFYは、ROW_NUMBER()、RANK()、DENSE_RANK()の3つのウィンドウ関数のみをサポートしています。

- QUALIFYを含むクエリの句の実行順序は、次の順序で評価されます。

1. FROM
2. WHERE
3. GROUP BY
4. HAVING
5. ウィンドウ
6. QUALIFY
7. DISTINCT
8. ORDER BY
9. LIMIT

### SUM()

指定された行の合計を計算します。

**構文:**

```SQL
SUM([DISTINCT] expr) [OVER (analytic_clause)]
```

`DISTINCT`はStarRocks v4.0からサポートされています。指定すると、SUM()はウィンドウ内の異なる値のみを合計します。

:::note
**ウィンドウフレームの制限:**

ウィンドウ関数としてSUM(DISTINCT)を使用する場合、RANGEフレームのみがサポートされます。ROWSフレームはサポートされません。
:::

**例**

これらの例では、[示例表](#ウィンドウ関数サンプルテーブル)`scores`のデータを使用します。

**例1: 基本的な使用法**

`subject`でデータをグループ化し、グループ内のすべての行のスコアの合計を計算します。

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

**例2: 全体ウィンドウでSUM(DISTINCT)を使用する**

すべての行の異なるスコアを合計します:

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER () AS distinct_sum
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_sum|
+----+---------+-------+-------------+
|  1 | math    |    80 |          255|
|  2 | math    |    85 |          255|
|  3 | math    |    80 |          255|
|  4 | english |    90 |          255|
|  5 | english |    85 |          255|
|  6 | english |    90 |          255|
+----+---------+-------+-------------+
```

異なる値の合計は255 (80 + 85 + 90) です。

**例 3: RANGEフレームを持つフレーム付きウィンドウでSUM(DISTINCT)を使用する**

RANGEフレームを使用して、各科目パーティション内で重複しないスコアを合計する:

```SQL
SELECT id, subject, score,
    SUM(DISTINCT score) OVER (
        PARTITION BY subject 
        ORDER BY score 
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS distinct_sum
FROM test_scores;
```

出力:

```plaintext
+----+---------+-------+-------------+
| id | subject | score | distinct_sum|
+----+---------+-------+-------------+
|  1 | math    |    80 |           80|
|  3 | math    |    80 |           80|
|  2 | math    |    85 |          165|
|  5 | english |    85 |           85|
|  4 | english |    90 |          175|
|  6 | english |    90 |          175|
+----+---------+-------+-------------+
```

各行について、この関数はパーティションの先頭から現在の行のスコア値まで（現在の行のスコア値を含む）の重複しないスコアを合計します。

### VARIANCE, VAR_POP, VARIANCE_POP

式の母分散を返します。VAR_POPとVARIANCE_POPはVARIANCEのエイリアスです。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、次のデータを使用します。[示例表](#ウィンドウ関数サンプルテーブル) `scores`。

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
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

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
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメーター:**

もし `expr` がテーブル列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

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
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメーター:**

もし `expr` がテーブル列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

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

2つの式のピアソン相関係数を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

:::tip
2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BY句とWindow句をサポートします。
:::

**パラメーター:**

もし `expr` がテーブル列である場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例**

この例では、[示例表](#ウィンドウ関数サンプルテーブル) `scores` のデータを使用します。

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
