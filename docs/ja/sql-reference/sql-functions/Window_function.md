---
displayed_sidebar: docs
---

# ウィンドウ関数

## 背景

ウィンドウ関数は、特別なクラスの組み込み関数です。集計関数と同様に、複数の入力行に対して計算を行い、単一のデータ値を取得します。違いは、ウィンドウ関数が特定のウィンドウ内で入力データを処理することで、「group by」メソッドを使用しないことです。各ウィンドウ内のデータは、over()句を使用してソートおよびグループ化できます。ウィンドウ関数は、**各行に対して個別の値を計算**し、各グループに対して1つの値を計算するのではありません。この柔軟性により、ユーザーはselect句に追加の列を追加し、結果セットをさらにフィルタリングすることができます。ウィンドウ関数は、selectリストおよび句の最外部の位置にのみ現れることができます。クエリの最後に効果を発揮し、つまり、`join`、`where`、および `group by` 操作が実行された後に適用されます。ウィンドウ関数は、トレンドの分析、外れ値の計算、大規模データのバケッティング分析によく使用されます。

## 使用法

ウィンドウ関数の構文:

```SQL
function(args) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
partition_by_clause ::= PARTITION BY expr [, expr ...]
order_by_clause ::= ORDER BY expr [ASC | DESC] [, expr [ASC | DESC] ...]
```

### 関数

現在サポートされている関数には以下が含まれます:

* MIN(), MAX(), COUNT(), SUM(), AVG()
* FIRST_VALUE(), LAST_VALUE(), LEAD(), LAG()
* ROW_NUMBER(), RANK(), DENSE_RANK(), QUALIFY()
* NTILE()
* VARIANCE(), VAR_SAMP(), STD(), STDDEV_SAMP(), COVAR_SAMP(), COVAR_POP(), CORR()

### PARTITION BY句

Partition By句はGroup Byに似ています。指定された1つ以上の列で入力行をグループ化します。同じ値を持つ行は一緒にグループ化されます。

### ORDER BY句

`Order By`句は基本的に外部の`Order By`と同じです。入力行の順序を定義します。`Partition By`が指定されている場合、`Order By`は各Partitionグループ内の順序を定義します。唯一の違いは、`OVER`句の`Order By n`（nは正の整数）が操作なしに等しいのに対し、外部の`Order By`の`n`はn番目の列でのソートを示します。

例:

この例では、eventsテーブルの`date_and_time`列でソートされた1, 2, 3などの値を持つid列をselectリストに追加します。

```SQL
SELECT row_number() OVER (ORDER BY date_and_time) AS id,
    c1, c2, c3, c4
FROM events;
```

### ウィンドウ句

ウィンドウ句は、操作のための行の範囲を指定するために使用されます（現在の行を基準にした前後の行）。以下の構文をサポートしています – AVG(), COUNT(), FIRST_VALUE(), LAST_VALUE() および SUM()。MAX() および MIN()の場合、ウィンドウ句は`UNBOUNDED PRECEDING`から開始を指定できます。

構文:

```SQL
ROWS BETWEEN [ { m | UNBOUNDED } PRECEDING | CURRENT ROW] [ AND [CURRENT ROW | { UNBOUNDED | n } FOLLOWING] ]
```

例:

次の株式データがあるとします。株式シンボルはJDRで、終値は日々の終値です。

```SQL
create table stock_ticker (
    stock_symbol string,
    closing_price decimal(8,2),
    closing_date timestamp);

-- ...データをロード...

select *
from stock_ticker
order by stock_symbol, closing_date
```

生データは次のように表示されます:

```plaintext
+--------------+---------------+---------------------+
| stock_symbol | closing_price | closing_date        |
+--------------+---------------+---------------------+
| JDR          | 12.86         | 2014-10-02 00:00:00 |
| JDR          | 12.89         | 2014-10-03 00:00:00 |
| JDR          | 12.94         | 2014-10-04 00:00:00 |
| JDR          | 12.55         | 2014-10-05 00:00:00 |
| JDR          | 14.03         | 2014-10-06 00:00:00 |
| JDR          | 14.75         | 2014-10-07 00:00:00 |
| JDR          | 13.98         | 2014-10-08 00:00:00 |
+--------------+---------------+---------------------+
```

このクエリは、ウィンドウ関数を使用して、3日間（前日、当日、翌日）の平均株価を持つmoving_average列を生成します。最初の日は前日の値がなく、最後の日は翌日の値がないため、これらの2行は2日間の平均値のみを計算します。ここで`Partition By`は効果を発揮しません。すべてのデータがJDRデータだからです。ただし、他の株式情報がある場合、`Partition By`はウィンドウ関数が各Partition内で操作されることを保証します。

```SQL
select stock_symbol, closing_date, closing_price,
    avg(closing_price)
        over (partition by stock_symbol
              order by closing_date
              rows between 1 preceding and 1 following
        ) as moving_average
from stock_ticker;
```

次のデータが得られます:

```plaintext
+--------------+---------------------+---------------+----------------+
| stock_symbol | closing_date        | closing_price | moving_average |
+--------------+---------------------+---------------+----------------+
| JDR          | 2014-10-02 00:00:00 | 12.86         | 12.87          |
| JDR          | 2014-10-03 00:00:00 | 12.89         | 12.89          |
| JDR          | 2014-10-04 00:00:00 | 12.94         | 12.79          |
| JDR          | 2014-10-05 00:00:00 | 12.55         | 13.17          |
| JDR          | 2014-10-06 00:00:00 | 14.03         | 13.77          |
| JDR          | 2014-10-07 00:00:00 | 14.75         | 14.25          |
| JDR          | 2014-10-08 00:00:00 | 13.98         | 14.36          |
+--------------+---------------------+---------------+----------------+
```

## 関数の例

このセクションでは、StarRocksでサポートされているウィンドウ関数について説明します。

### AVG()

構文:

```SQL
AVG(expr) [OVER (*analytic_clause*)]
```

例:

現在の行とその前後の行のx平均を計算します。

```SQL
select x, property,
    avg(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving average'
from int_t
where property in ('odd','even');
```

```plaintext
+----+----------+----------------+
| x  | property | moving average |
+----+----------+----------------+
| 2  | even     | 3              |
| 4  | even     | 4              |
| 6  | even     | 6              |
| 8  | even     | 8              |
| 10 | even     | 9              |
| 1  | odd      | 2              |
| 3  | odd      | 3              |
| 5  | odd      | 5              |
| 7  | odd      | 7              |
| 9  | odd      | 8              |
+----+----------+----------------+
```

### COUNT()

構文:

```SQL
COUNT(expr) [OVER (analytic_clause)]
```

例:

現在の行から最初の行までのxの出現回数を数えます。

```SQL
select x, property,
    count(x)
        over (
            partition by property
            order by x
            rows between unbounded preceding and current row
        ) as 'cumulative total'
from int_t where property in ('odd','even');
```

```plaintext
+----+----------+------------------+
| x  | property | cumulative count |
+----+----------+------------------+
| 2  | even     | 1                |
| 4  | even     | 2                |
| 6  | even     | 3                |
| 8  | even     | 4                |
| 10 | even     | 5                |
| 1  | odd      | 1                |
| 3  | odd      | 2                |
| 5  | odd      | 3                |
| 7  | odd      | 4                |
| 9  | odd      | 5                |
+----+----------+------------------+
```

### DENSE_RANK()

DENSE_RANK()関数はランキングを表すために使用されます。RANK()とは異なり、DENSE_RANK()は**空白のない**番号を持ちます。たとえば、1位が2つある場合、DENSE_RANK()の3番目の番号は2のままですが、RANK()の3番目の番号は3になります。

構文:

```SQL
DENSE_RANK() OVER(partition_by_clause order_by_clause)
```

次の例は、property列のグループ化に基づいて列xのランキングを示しています。

```SQL
select x, y,
    dense_rank()
        over (
            partition by x
            order by y
        ) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 2    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 1    |
| 3 | 2 | 2    |
+---+---+------+
```

### NTILE

NTILE()関数は、指定された`num_buckets`の数でパーティション内のソートされた行をできるだけ均等に分割し、分割された行をそれぞれのバケットに格納し、1から始まる`[1, 2, ..., num_buckets]`のバケット番号を返します。

バケットのサイズについて:

* 行数が指定された`num_buckets`の数で正確に割り切れる場合、すべてのバケットは同じサイズになります。
* 行数が指定された`num_buckets`の数で正確に割り切れない場合、2つの異なるサイズのバケットが存在します。サイズの差は1です。より多くの行を持つバケットは、より少ない行を持つバケットの前にリストされます。

構文:

```SQL
NTILE (num_buckets) OVER (partition_by_clause order_by_clause)
```

`num_buckets`: 作成されるバケットの数。値は定数の正の整数で、最大値は`2^63 - 1`です。

NTILE()関数ではウィンドウ句は許可されていません。

NTILE()関数はBIGINT型のデータを返します。

例:

次の例では、パーティション内のすべての行を2つのバケットに分割します。

```sql
select id, x, y,
    ntile(2)
        over (
            partition by x
            order by y
        ) as bucket_id
from t1;
```

返されるデータ:

```plaintext
+------+------+------+-----------+
| id   | x    | y    | bucket_id |
+------+------+------+-----------+
|    1 |    1 |   11 |         1 |
|    2 |    1 |   11 |         1 |
|    3 |    1 |   22 |         1 |
|    4 |    1 |   33 |         2 |
|    5 |    1 |   44 |         2 |
|    6 |    1 |   55 |         2 |
|    7 |    2 |   66 |         1 |
|    8 |    2 |   77 |         1 |
|    9 |    2 |   88 |         2 |
|   10 |    3 |   99 |         1 |
+------+------+------+-----------+
```

上記の例のように、`num_buckets`が`2`の場合:

* No.1からNo.6の行は最初のパーティションに分類され、No.1からNo.3の行は最初のバケットに格納され、No.4からNo.6の行は2番目のバケットに格納されます。
* No.7からNo.9の行は2番目のパーティションに分類され、No.7とNo.8の行は最初のバケットに格納され、No.9の行は2番目のバケットに格納されます。
* No.10の行は3番目のパーティションに分類され、最初のバケットに格納されます。

### FIRST_VALUE()

FIRST_VALUE()はウィンドウ範囲の**最初**の値を返します。

構文:

```SQL
FIRST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0からサポートされています。これは、`expr`のNULL値を計算から除外するかどうかを決定するために使用されます。デフォルトでは、NULL値が含まれており、フィルタリングされた結果の最初の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定すると、フィルタリングされた結果の最初の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

例:

次のデータがあります:

```SQL
 select name, country, greeting
 from mail_merge;
 ```

```plaintext
+---------+---------+--------------+
| name    | country | greeting     |
+---------+---------+--------------+
| Pete    | USA     | Hello        |
| John    | USA     | Hi           |
| Boris   | Germany | Guten tag    |
| Michael | Germany | Guten morgen |
| Bjorn   | Sweden  | Hej          |
| Mats    | Sweden  | Tja          |
+---------+---------+--------------+
```

FIRST_VALUE()を使用して、国ごとのグループ化に基づいて、各グループの最初の挨拶の値を返します。

```SQL
select country, name,
    first_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
```

```plaintext
+---------+---------+-----------+
| country | name    | greeting  |
+---------+---------+-----------+
| Germany | Boris   | Guten tag |
| Germany | Michael | Guten tag |
| Sweden  | Bjorn   | Hej       |
| Sweden  | Mats    | Hej       |
| USA     | John    | Hi        |
| USA     | Pete    | Hi        |
+---------+---------+-----------+
```

### LAG()

現在の行の前に`offset`行遅れた行の値を返します。この関数は、行間の値を比較し、データをフィルタリングするためによく使用されます。

`LAG()`は、次のタイプのデータをクエリするために使用できます:

* 数値: TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, DECIMAL
* 文字列: CHAR, VARCHAR
* 日付: DATE, DATETIME
* BITMAPおよびHLLはStarRocks 2.5からサポートされています。

構文:

```Haskell
LAG(expr[, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

パラメータ:

* `expr`: 計算したいフィールド。
* `offset`: オフセット。**正の整数**でなければなりません。このパラメータが指定されていない場合、デフォルトは1です。
* `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されていない場合、デフォルトはNULLです。`default`は、`expr`と互換性のある型の任意の式をサポートします。

例:

前日の`closing_price`を計算します。この例では、`default`が0に設定されており、一致する行が見つからない場合は0が返されます。

```SQL
select stock_symbol, closing_date, closing_price,
    lag(closing_price, 1, 0)
    over(
    partition by stock_symbol
    order by closing_date
    ) as "yesterday closing"
from stock_ticker
order by closing_date;
```

出力:

```plaintext
+--------------+---------------------+---------------+-------------------+
| stock_symbol | closing_date        | closing_price | yesterday closing |
+--------------+---------------------+---------------+-------------------+
| JDR          | 2014-09-13 00:00:00 | 12.86         | 0                 |
| JDR          | 2014-09-14 00:00:00 | 12.89         | 12.86             |
| JDR          | 2014-09-15 00:00:00 | 12.94         | 12.89             |
| JDR          | 2014-09-16 00:00:00 | 12.55         | 12.94             |
| JDR          | 2014-09-17 00:00:00 | 14.03         | 12.55             |
| JDR          | 2014-09-18 00:00:00 | 14.75         | 14.03             |
| JDR          | 2014-09-19 00:00:00 | 13.98         | 14.75             |
+--------------+---------------------+---------------+-------------------+
```

最初の行は、前の行がない場合に何が起こるかを示しています。関数は***`default`***値0を返します。

### LAST_VALUE()

LAST_VALUE()はウィンドウ範囲の**最後**の値を返します。これはFIRST_VALUE()の反対です。

構文:

```SQL
LAST_VALUE(expr [IGNORE NULLS]) OVER(partition_by_clause order_by_clause [window_clause])
```

`IGNORE NULLS`はv2.5.0からサポートされています。これは、`expr`のNULL値を計算から除外するかどうかを決定するために使用されます。デフォルトでは、NULL値が含まれており、フィルタリングされた結果の最後の値がNULLの場合、NULLが返されます。IGNORE NULLSを指定すると、フィルタリングされた結果の最後の非NULL値が返されます。すべての値がNULLの場合、IGNORE NULLSを指定してもNULLが返されます。

例のデータを使用します:

```SQL
select country, name,
    last_value(greeting)
        over (
            partition by country
            order by name, greeting
        ) as greeting
from mail_merge;
```

```plaintext
+---------+---------+--------------+
| country | name    | greeting     |
+---------+---------+--------------+
| Germany | Boris   | Guten morgen |
| Germany | Michael | Guten morgen |
| Sweden  | Bjorn   | Tja          |
| Sweden  | Mats    | Tja          |
| USA     | John    | Hello        |
| USA     | Pete    | Hello        |
+---------+---------+--------------+
```

### LEAD()

現在の行の後に`offset`行進んだ行の値を返します。この関数は、行間の値を比較し、データをフィルタリングするためによく使用されます。

`lead()`でクエリできるデータ型は、[lag()](#lag)でサポートされているものと同じです。

構文

```Haskell
LEAD(expr[, offset[, default]])
OVER([<partition_by_clause>] [<order_by_clause>])
```

パラメータ:

* `expr`: 計算したいフィールド。
* `offset`: オフセット。正の整数でなければなりません。このパラメータが指定されていない場合、デフォルトは1です。
* `default`: 一致する行が見つからない場合に返されるデフォルト値。このパラメータが指定されていない場合、デフォルトはNULLです。`default`は、`expr`と互換性のある型の任意の式をサポートします。

例:

2日間の終値のトレンドを計算します。つまり、翌日の価格が高いか低いかを示します。`default`は0に設定されており、一致する行が見つからない場合は0が返されます。

```SQL
select stock_symbol, closing_date, closing_price,
    case(lead(closing_price, 1, 0) 
         over (partition by stock_symbol
         order by closing_date)- closing_price) > 0 
        when true then "higher"
        when false then "flat or lower" end
    as "trending"
from stock_ticker
order by closing_date;
```

出力

```plaintext
+--------------+---------------------+---------------+---------------+
| stock_symbol | closing_date        | closing_price | trending      |
+--------------+---------------------+---------------+---------------+
| JDR          | 2014-09-13 00:00:00 | 12.86         | higher        |
| JDR          | 2014-09-14 00:00:00 | 12.89         | higher        |
| JDR          | 2014-09-15 00:00:00 | 12.94         | flat or lower |
| JDR          | 2014-09-16 00:00:00 | 12.55         | higher        |
| JDR          | 2014-09-17 00:00:00 | 14.03         | higher        |
| JDR          | 2014-09-18 00:00:00 | 14.75         | flat or lower |
| JDR          | 2014-09-19 00:00:00 | 13.98         | flat or lower |
+--------------+---------------------+---------------+---------------+
```

### MAX()

現在のウィンドウ内の指定された行の最大値を返します。

構文

```SQL
MAX(expr) [OVER (analytic_clause)]
```

例:

最初の行から現在の行の後の行までの最大値を計算します。

```SQL
select x, property,
    max(x)
        over (
            order by property, x
            rows between unbounded preceding and 1 following
        ) as 'local maximum'
from int_t
where property in ('prime','square');
```

```plaintext
+---+----------+---------------+
| x | property | local maximum |
+---+----------+---------------+
| 2 | prime    | 3             |
| 3 | prime    | 5             |
| 5 | prime    | 7             |
| 7 | prime    | 7             |
| 1 | square   | 7             |
| 4 | square   | 9             |
| 9 | square   | 9             |
+---+----------+---------------+
```

StarRocks 2.4以降、`rows between n preceding and n following`として行範囲を指定できるようになりました。これにより、現在の行の前の`n`行と後の`n`行をキャプチャできます。

例文:

```sql
select x, property,
    max(x)
        over (
            order by property, x
            rows between 3 preceding and 2 following) as 'local maximum'
from int_t
where property in ('prime','square');
```

### MIN()

現在のウィンドウ内の指定された行の最小値を返します。

構文:

```SQL
MIN(expr) [OVER (analytic_clause)]
```

例:

最初の行から現在の行の後の行までの最小値を計算します。

```SQL
select x, property,
    min(x)
        over (
            order by property, x desc
            rows between unbounded preceding and 1 following
        ) as 'local minimum'
from int_t
where property in ('prime','square');
```

```plaintext
+---+----------+---------------+
| x | property | local minimum |
+---+----------+---------------+
| 7 | prime    | 5             |
| 5 | prime    | 3             |
| 3 | prime    | 2             |
| 2 | prime    | 2             |
| 9 | square   | 2             |
| 4 | square   | 1             |
| 1 | square   | 1             |
+---+----------+---------------+
```

StarRocks 2.4以降、`rows between n preceding and n following`として行範囲を指定できるようになりました。これにより、現在の行の前の`n`行と後の`n`行をキャプチャできます。

例文:

```sql
select x, property,
    min(x)
    over (
          order by property, x desc
          rows between 3 preceding and 2 following) as 'local minimum'
from int_t
where property in ('prime','square');
```

### RANK()

RANK()関数はランキングを表すために使用されます。DENSE_RANK()とは異なり、RANK()は**空白のある**番号が現れます。たとえば、1位が2つある場合、RANK()の3番目の番号は2ではなく3になります。

構文:

```SQL
RANK() OVER(partition_by_clause order_by_clause)
```

例:

列xに基づいてランキングします:

```SQL
select x, y, rank() over(partition by x order by y) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 2    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 1    |
| 3 | 2 | 3    |
+---+---+------+
```

### ROW_NUMBER()

各パーティションの行に対して1から始まる連続した整数を返します。RANK()やDENSE_RANK()とは異なり、ROW_NUMBER()によって返される値は**繰り返しやギャップがなく**、**連続して増加**します。

構文:

```SQL
ROW_NUMBER() OVER(partition_by_clause order_by_clause)
```

例:

```SQL
select x, y, row_number() over(partition by x order by y) as `rank`
from int_t;
```

```plaintext
+---+---+------+
| x | y | rank |
+---+---+------+
| 1 | 1 | 1    |
| 1 | 2 | 2    |
| 1 | 2 | 3    |
| 2 | 1 | 1    |
| 2 | 2 | 2    |
| 2 | 3 | 3    |
| 3 | 1 | 1    |
| 3 | 1 | 2    |
| 3 | 2 | 3    |
+---+---+------+
```

### QUALIFY()

QUALIFY句はウィンドウ関数の結果をフィルタリングします。SELECT文で、QUALIFY句を使用して列に条件を適用し、結果をフィルタリングできます。QUALIFYは集計関数のHAVING句に類似しています。この関数はv2.5からサポートされています。

QUALIFYはSELECT文の記述を簡素化します。

QUALIFYが使用される前は、SELECT文は次のようになるかもしれません:

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

`<window_function>`: `QUALIFY`句の後にはウィンドウ関数のみが続くことができます。ROW_NUMBER(), RANK(), および DENSE_RANK()を含みます。

**例:**

```SQL
-- テーブルを作成します。
CREATE TABLE sales_record (
   city_id INT,
   item STRING,
   sales INT
) DISTRIBUTED BY HASH(`city_id`) BUCKETS 1;

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

例3: テーブルの各パーティションから売上が1位のレコードを取得します。テーブルは`item`で2つのパーティションに分割され、各パーティションで最も売上が高い行が返されます。

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

QUALIFYを含むクエリの句の実行順序は次の順序で評価されます:

> 1. From
> 2. Where
> 3. Group by
> 4. Having
> 5. Window
> 6. QUALIFY
> 7. Distinct
> 8. Order by
> 9. Limit

### SUM()

構文:

```SQL
SUM(expr) [OVER (analytic_clause)]
```

例:

propertyでグループ化し、グループ内の**現在、前、後の行**の合計を計算します。

```SQL
select x, property,
    sum(x)
        over (
            partition by property
            order by x
            rows between 1 preceding and 1 following
        ) as 'moving total'
from int_t where property in ('odd','even');
```

```plaintext
+----+----------+--------------+
| x  | property | moving total |
+----+----------+--------------+
| 2  | even     | 6            |
| 4  | even     | 12           |
| 6  | even     | 18           |
| 8  | even     | 24           |
| 10 | even     | 18           |
| 1  | odd      | 4            |
| 3  | odd      | 9            |
| 5  | odd      | 15           |
| 7  | odd      | 21           |
+----+----------+--------------+
```

### VARIANCE, VAR_POP, VARIANCE_POP

式の母分散を返します。VAR_POPおよびVARIANCE_POPはVARIANCEの別名です。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```SQL
VARIANCE(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

VARIANCE()関数を使用します。

```plaintext
mysql> select variance(k) over (partition by no) FROM agg;
+-------------------------------------+
| variance(k) OVER (PARTITION BY no ) |
+-------------------------------------+
|                                   0 |
|                             54.6875 |
|                             54.6875 |
|                             54.6875 |
|                             54.6875 |
+-------------------------------------+

mysql> select variance(k) over(
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+-------------------+
| window_test       |
+-------------------+
|                 0 |
|                25 |
| 38.88888888888889 |
|           54.6875 |
|           54.6875 |
+-------------------+
```

### VAR_SAMP, VARIANCE_SAMP

式のサンプル分散を返します。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
VAR_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

VAR_SAMP()ウィンドウ関数を使用します。

```plaintext
mysql> select VAR_SAMP(k) over (partition by no) FROM agg;
+-------------------------------------+
| var_samp(k) OVER (PARTITION BY no ) |
+-------------------------------------+
|                                   0 |
|                   72.91666666666667 |
|                   72.91666666666667 |
|                   72.91666666666667 |
|                   72.91666666666667 |
+-------------------------------------+

mysql> select VAR_SAMP(k) over(
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+--------------------+
| window_test        |
+--------------------+
|                  0 |
|                 50 |
| 58.333333333333336 |
|  72.91666666666667 |
|  72.91666666666667 |
+--------------------+
```

### STD, STDDEV, STDDEV_POP

式の標準偏差を返します。これらの関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
STD(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

STD()ウィンドウ関数を使用します。

```plaintext
mysql> select STD(k) over (partition by no) FROM agg;
+--------------------------------+
| std(k) OVER (PARTITION BY no ) |
+--------------------------------+
|                              0 |
|               7.39509972887452 |
|               7.39509972887452 |
|               7.39509972887452 |
|               7.39509972887452 |
+--------------------------------+

mysql> select std(k) over (
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+-------------------+
| window_test       |
+-------------------+
|                 0 |
|                 5 |
| 6.236095644623236 |
|  7.39509972887452 |
|  7.39509972887452 |
+-------------------+
```

### STDDEV_SAMP

式のサンプル標準偏差を返します。この関数はv2.5.10以降、ウィンドウ関数として使用できます。

**構文:**

```sql
STDDEV_SAMP(expr) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

STDDEV_SAMP()ウィンドウ関数を使用します。

```plaintext
mysql> select STDDEV_SAMP(k) over (partition by no) FROM agg;
+----------------------------------------+
| stddev_samp(k) OVER (PARTITION BY no ) |
+----------------------------------------+
|                                      0 |
|                      8.539125638299666 |
|                      8.539125638299666 |
|                      8.539125638299666 |
|                      8.539125638299666 |
+----------------------------------------+

mysql> select STDDEV_SAMP(k) over (
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+--------------------+
| window_test        |
+--------------------+
|                  0 |
| 7.0710678118654755 |
|  7.637626158259733 |
|  8.539125638299666 |
|  8.539125638299666 |
+--------------------+
```

### COVAR_SAMP

2つの式のサンプル共分散を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
COVAR_SAMP(expr1,expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

COVAR_SAMP()ウィンドウ関数を使用します。

```plaintext
mysql> select COVAR_SAMP(k, v) over (partition by no) FROM agg;
+------------------------------------------+
| covar_samp(k, v) OVER (PARTITION BY no ) |
+------------------------------------------+
|                                     NULL |
|                       119.99999999999999 |
|                       119.99999999999999 |
|                       119.99999999999999 |
|                       119.99999999999999 |
+------------------------------------------+

mysql> select COVAR_SAMP(k,v) over (
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+--------------------+
| window_test        |
+--------------------+
|               NULL |
|                 55 |
|                 55 |
| 119.99999999999999 |
| 119.99999999999999 |
+--------------------+
```

### COVAR_POP

2つの式の母共分散を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
COVAR_POP(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

COVAR_POP()ウィンドウ関数を使用します。

```plaintext
mysql> select COVAR_POP(k, v) over (partition by no) FROM agg;
+-----------------------------------------+
| covar_pop(k, v) OVER (PARTITION BY no ) |
+-----------------------------------------+
|                                    NULL |
|                       79.99999999999999 |
|                       79.99999999999999 |
|                       79.99999999999999 |
|                       79.99999999999999 |
+-----------------------------------------+

mysql> select COVAR_POP(k,v) over (
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+-------------------+
| window_test       |
+-------------------+
|              NULL |
|              27.5 |
|              27.5 |
| 79.99999999999999 |
| 79.99999999999999 |
+-------------------+
```

### CORR

2つの式間のピアソン相関係数を返します。この関数はv2.5.10からサポートされています。また、集計関数でもあります。

**構文:**

```sql
CORR(expr1, expr2) OVER([partition_by_clause] [order_by_clause] [order_by_clause window_clause])
```

> **注意**
>
> 2.5.13、3.0.7、3.1.4以降、このウィンドウ関数はORDER BYおよびウィンドウ句をサポートしています。

**パラメータ:**

`expr`がテーブル列の場合、TINYINT、SMALLINT、INT、BIGINT、LARGEINT、FLOAT、DOUBLE、またはDECIMALに評価される必要があります。

**例:**

テーブル`agg`に次のデータがあるとします:

```plaintext
mysql> select * from agg;
+------+-------+-------+
| no   | k     | v     |
+------+-------+-------+
|    1 | 10.00 |  NULL |
|    2 | 10.00 | 11.00 |
|    2 | 20.00 | 22.00 |
|    2 | 25.00 |  NULL |
|    2 | 30.00 | 35.00 |
+------+-------+-------+
```

CORR()ウィンドウ関数を使用します。

```plaintext
mysql> select CORR(k, v) over (partition by no) FROM agg;
+------------------------------------+
| corr(k, v) OVER (PARTITION BY no ) |
+------------------------------------+
|                               NULL |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
|                 0.9988445981121532 |
+------------------------------------+

mysql> select CORR(k,v) over (
    partition by no
    order by k
    rows between unbounded preceding and 1 following) AS window_test
FROM agg order by no,k;
+--------------------+
| window_test        |
+--------------------+
|               NULL |
|                  1 |
|                  1 |
| 0.9988445981121532 |
| 0.9988445981121532 |
+--------------------+
```