---
displayed_sidebar: docs
---

# SELECT

SELECT は、1 つ以上のテーブル、ビュー、またはマテリアライズドビューからデータをクエリします。SELECT ステートメントは、通常、次の句で構成されます。

- [WITH](#with)
- [Join](#join)
- [ORDER BY](#order-by)
- [GROUP BY](#group-by)
- [HAVING](#having)
- [LIMIT](#limit)
- [OFFSET](#offset)
- [UNION](#union)
- [INTERSECT](#intersect)
- [EXCEPT/MINUS](#exceptminus)
- [DISTINCT](#distinct)
- [サブクエリ](#サブクエリ)
- [WHERE と演算子](#Where-と-Operator)
- [エイリアス](#エイリアス)
- [PIVOT](#pivot)
- [EXCLUDE](#exclude)
- [RECURSIVE](#recursive)

SELECT は、独立したステートメントまたは他のステートメントにネストされた句として機能します。SELECT 句の出力は、他のステートメントの入力として使用できます。

StarRocks のクエリステートメントは、基本的に SQL92 標準に準拠しています。サポートされている SELECT の使用方法について簡単に説明します。

> **NOTE**
>
> StarRocks 内部テーブルのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、これらのオブジェクトに対する SELECT 権限が必要です。外部データソースのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、対応する external catalog に対する USAGE 権限が必要です。

## WITH

SELECT ステートメントの前に追加できる句で、SELECT 内で複数回参照される複雑な式のエイリアスを定義します。

CREATE VIEW と似ていますが、句で定義されたテーブル名とカラム名はクエリ終了後も保持されず、実際のテーブルまたは VIEW の名前と競合しません。

WITH 句を使用する利点は次のとおりです。

便利でメンテナンスが容易で、クエリ内の重複を減らすことができます。

クエリの最も複雑な部分を個別のブロックに抽象化することで、SQL コードをより簡単に読み、理解することができます。

例：

```sql
-- Define one subquery at the outer level, and another at the inner level as part of the initial stage of the UNION ALL query.

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

## Join

Join （ジョイン）オペレーションは、2つ以上のテーブルからデータを結合し、それらのテーブルからいくつかのカラムの結果セットを返します。

StarRocks は、以下のジョインをサポートしています。
- [Self Join](#self-join)
- [Cross Join](#cross-join)
- [Inner Join](#inner-join)
- [Outer Join](#outer-join) （Left Join、Right Join、Full Join を含む）
- [Semi Join](#semi-join)
- [Anti Join](#anti-join)
- [Equi-join and Non-equi-join](#equi-join-と-non-equi-join)
- [USING 句を使用した Join](#using-句を使用した-Join)
- [ASOF Join](#asof-join)

構文:

```sql
SELECT select_list FROM
table_or_subquery1 [INNER] JOIN table_or_subquery2 |
table_or_subquery1 {LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER]} JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} SEMI JOIN table_or_subquery2 |
table_or_subquery1 {LEFT | RIGHT} ANTI JOIN table_or_subquery2 |
[ ON col1 = col2 [AND col3 = col4 ...] |
USING (col1 [, col2 ...]) ]
[other_join_clause ...]
[ WHERE where_clauses ]
```

```sql
SELECT select_list FROM
table_or_subquery1, table_or_subquery2 [, table_or_subquery3 ...]
[other_join_clause ...]
WHERE
col1 = col2 [AND col3 = col4 ...]
```

```sql
SELECT select_list FROM
table_or_subquery1 CROSS JOIN table_or_subquery2
[other_join_clause ...]
[ WHERE where_clauses ]
```

### Self Join

StarRocks は、Self Join をサポートしています。たとえば、同じテーブルの異なるカラムが Join されます。

実際には、Self Join を識別するための特別な構文はありません。Self Join における Join の両側の条件は、同じテーブルから来ています。

異なるエイリアスを割り当てる必要があります。

例：

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

### Cross Join

Cross Join は大量の結果を生成する可能性があるため、使用には注意が必要です。

どうしても Cross Join を使用する必要がある場合でも、フィルタ条件を使用し、返される結果が少なくなるようにする必要があります。例：

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

### Inner Join

Inner Join は、最もよく知られ、一般的に使用される Join です。2 つの類似したテーブルから要求された列の結果を返し、両方のテーブルの列に同じ値が含まれている場合に Join されます。

両方のテーブルの列名が同じ場合は、完全名 (table_name.column_name の形式) を使用するか、列名にエイリアスを付ける必要があります。

例：

次の 3 つのクエリは同等です。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

### Outer Join

Outer Join は、左または右のテーブル、または両方のテーブルのすべての行を返します。別のテーブルに一致するデータがない場合は、NULL に設定します。例：

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

### 等価 Join と不等 Join

通常、等価 Join が最も一般的に使用される結合です。結合条件の演算子として等号が必要です。

不等 Join は結合条件として `!=` を使用します。不等 Join は大量の結果を生成し、計算中にメモリ制限を超える可能性があります。

使用には注意が必要です。不等 Join は Inner Join のみをサポートします。例：

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

### Semi Join

Left Semi Join は、右テーブルのデータと一致する左テーブルの行のみを返します。右テーブルの行がいくつ一致するかは関係ありません。

左テーブルのこの行は、最大で1回返されます。Right Semi Join も同様に機能しますが、返されるデータは右テーブルになります。

例：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

### Anti Join

Left Anti Join は、右側のテーブルに一致しない左側のテーブルの行のみを返します。

Right Anti Join はこの比較を逆にして、左側のテーブルに一致しない右側のテーブルの行のみを返します。例：

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

### Equi-join と Non-equi-join

StarRocks がサポートするさまざまなジョインは、ジョインで指定されたジョイン条件に応じて、Equi-join と Non-equi-join に分類できます。

| **Join タイプ** | **バリアント**                                                      |
| -------------- | ----------------------------------------------------------------- |
| Equi-join      | Self join、cross join、inner join、outer join、semi join、anti join |
| Non-equi-join  | cross join、inner join、left semi join、left anti join、outer join  |

- Equi-join
  
  Equi-join は、2 つのジョインアイテムが `=` 演算子で結合されるジョイン条件を使用します。例：`a JOIN b ON a.id = b.id`。

- Non-equi-join
  
  Non-equi-join は、2 つのジョインアイテムが `<`、`<=`、`>`、`>=`、`<>` などの比較演算子で結合されるジョイン条件を使用します。例：`a JOIN b ON a.id < b.id`。Non-equi-join は Equi-join よりも実行速度が遅くなります。Non-equi-join を使用する場合は注意することをお勧めします。

  次の 2 つの例は、Non-equi-join を実行する方法を示しています。

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

### USING 句を使用した Join

v4.0.2以降、StarRocks は `ON` に加えて、`USING` 句によるジョイン条件の指定をサポートしています。これにより、同じ名前の列を持つ等価ジョインを簡素化できます。例：`SELECT * FROM t1 JOIN t2 USING (id)`。

**バージョン間の違い：**

- **v4.0.2より前のバージョン**
  
  `USING` は構文糖として扱われ、内部的に `ON` 条件に変換されます。結果には、左と右のテーブル両方からの USING 列が個別の列として含まれ、USING 列を参照する際にはテーブルエイリアス修飾子（例：`t1.id`）が許可されていました。

  例：

  ```SQL
  SELECT t1.id, t2.id FROM t1 JOIN t2 USING (id);  -- Returns two separate id columns
  ```

- **v4.0.2 以降**
  
  StarRocks は、SQL 標準の `USING` セマンティクスを実装しています。主な機能は次のとおりです。
  
  - `FULL OUTER JOIN` を含む、すべてのジョインタイプがサポートされています。
  - USING カラムは、結果に単一の結合されたカラムとして表示されます。FULL OUTER JOIN の場合、`COALESCE(left.col, right.col)` セマンティクスが使用されます。
  - テーブルエイリアス修飾子 (例: `t1.id`) は、USING カラムではサポートされなくなりました。非修飾カラム名 (例: `id`) を使用する必要があります。
  - `SELECT *` の結果の場合、カラムの順序は `[USING カラム, 左側の非 USING カラム, 右側の非 USING カラム]` となります。

  例：

  ```SQL
  SELECT t1.id FROM t1 JOIN t2 USING (id);        -- ❌ Error: Column 'id' is ambiguous
  SELECT id FROM t1 JOIN t2 USING (id);           -- ✅ Correct: Returns a single coalesced 'id' column
  SELECT * FROM t1 FULL OUTER JOIN t2 USING (id); -- ✅ FULL OUTER JOIN is supported
  ```

これらの変更は、StarRocks の動作を SQL 標準に準拠したデータベースに合わせるものです。

### ASOF Join

ASOF Join は、時系列分析で一般的に使用される時間ベースまたは範囲ベースのジョインの一種です。特定のキーが等しいことと、時間フィールドまたはシーケンスフィールドに関する不等式条件（例：`t1.time >= t2.time`）に基づいて、2 つのテーブルをジョインできます。ASOF Join は、左側のテーブルの各行に対して、右側のテーブルから最も新しい一致する行を選択します。v4.0 以降でサポートされています。

実際のシナリオでは、時系列データを含む分析は、多くの場合、次の課題に直面します。
- データ収集タイミングのずれ（異なるセンサーのサンプリング時間など）
- イベントの発生時刻と記録時刻のわずかなずれ
- 特定のタイムスタンプに最も近い過去のレコードを見つける必要性

従来の等価ジョイン（INNER Join）では、このようなデータを処理する際にデータが大幅に失われることがよくあります。一方、不等式ジョインではパフォーマンスの問題が発生する可能性があります。ASOF Join は、これらの特定の課題に対処するために設計されました。

ASOF Join は、一般的に次のような場合に使用されます。

- **金融市場分析**
  - 株価と取引量のマッチング
  - 異なる市場からのデータの調整
  - デリバティブ価格の参照データマッチング
- **IoT データ処理**
  - 複数のセンサーデータストリームの調整
  - デバイスの状態変化の相関付け
  - 時系列データの補間
- **ログ分析**
  - システムイベントとユーザーアクションの相関付け
  - 異なるサービスからのログのマッチング
  - 障害分析と問題追跡

構文：

```SQL
SELECT [select_list]
FROM left_table [AS left_alias]
ASOF LEFT JOIN right_table [AS right_alias]
    ON equality_condition
    AND asof_condition
[WHERE ...]
[ORDER BY ...]
```

- `ASOF LEFT JOIN`: 時間またはシーケンスで最も近い一致に基づいて、非等価ジョインを実行します。 ASOF LEFT JOIN は、左側のテーブルからすべての行を返し、一致しない右側の行を NULL で埋めます。
- `equality_condition`: 標準的な等価制約（たとえば、ティッカーシンボルまたは ID の一致）。
- `asof_condition`: 通常 `left.time >= right.time` として記述される範囲条件で、`left.time` を超えない最新の `right.time` レコードを検索することを示します。

:::note
`asof_condition` では DATE 型と DATETIME 型のみがサポートされています。また、サポートされる `asof_condition` は 1 つのみです。
:::

例：

```SQL
SELECT *
FROM holdings h ASOF LEFT JOIN prices p             
ON h.ticker = p.ticker            
AND h.when >= p.when
ORDER BY ALL;
```

制限事項：

- 現在、Inner Join (デフォルト) と Left Outer Join のみがサポートされています。
- `asof_condition` では、DATE 型と DATETIME 型のみがサポートされています。
- `asof_condition` は 1 つのみサポートされています。

## ORDER BY

SELECTステートメントのORDER BY句は、1つまたは複数のカラムの値を比較して、結果セットをソートします。

ORDER BYは、すべての結果を1つのノードに送信してマージしてからソートする必要があるため、時間とリソースを消費する操作です。ソートは、ORDER BYなしのクエリよりも多くのメモリリソースを消費します。

したがって、ソートされた結果セットから最初の`N`個の結果のみが必要な場合は、LIMIT句を使用できます。これにより、メモリ使用量とネットワークオーバーヘッドが削減されます。LIMIT句が指定されていない場合、デフォルトで最初の65535個の結果が返されます。

**構文**

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

**パラメータ**

- `ASC` は、結果を昇順で返すように指定します。
- `DESC` は、結果を降順で返すように指定します。順序が指定されていない場合、デフォルトは ASC （昇順）です。
- `NULLS FIRST` は、NULL 値を非 NULL 値よりも前に返すことを示します。
- `NULLS LAST` は、NULL 値を非 NULL 値よりも後に返すことを示します。

**例**

```sql
select * from big_table order by tiny_column, short_column desc;
select  *  from  sales_record  order by  employee_id  nulls first;
```

## GROUP BY

GROUP BY句は、集計関数と組み合わせて使用されることがよくあります。GROUP BY句で指定された列は、集計演算には参加しません。

**構文**

```sql
SELECT
...
aggregate_function() [ FILTER ( where boolean_expression ) ]
...
FROM ...
[ ... ]
GROUP BY [
    , ... |
    GROUPING SETS [, ...] (  groupSet [ , groupSet [ , ... ] ] ) |
    ROLLUP(expr  [ , expr [ , ... ] ]) |
    CUBE(expr  [ , expr [ , ... ] ])
    ]
[ ... ]
```

**パラメータ**

- `FILTER` は、集計関数と一緒に使用できます。フィルターされた行のみが集計関数の計算に参加します。

  > **NOTE**
  >
  > - FILTER句は、AVG、COUNT、MAX、MIN、SUM、ARRAY_AGG、およびARRAY_AGG_DISTINCT関数でのみサポートされています。
  > - FILTER句は、COUNT DISTINCTではサポートされていません。
  > - FILTER句が指定されている場合、ARRAY_AGG関数およびARRAY_AGG_DISTINCT関数内ではORDER BY句は許可されません。

- `GROUPING SETS` 、 `CUBE` 、および `ROLLUP` は、GROUP BY句の拡張です。 GROUP BY句では、複数のセットのグループ化された集計を実現するために使用できます。結果は、複数のGROUP BY句のUNIONの結果と同等です。

**例**

例1： `FILTER`

  次の2つのクエリは同等です。

  ```sql
  SELECT
    COUNT(*) AS total_users,
    SUM(CASE WHEN gender = 'M' THEN 1 ELSE 0 END) AS male_users,
    SUM(CASE WHEN gender = 'F' THEN 1 ELSE 0 END) AS female_users
  FROM users;
  ```

  ```sql
  SELECT
    COUNT(*) AS total_users,
    COUNT(*) FILTER (WHERE gender = 'M') AS male_users,
    COUNT(*) FILTER (WHERE gender = 'F') AS female_users
  FROM users;
  ```

例2：`GROUPING SETS`、`CUBE`、および `ROLLUP`

`ROLLUP(a,b,c)` は、次の `GROUPING SETS` 句と同等です。

    ```sql
    GROUPING SETS (
    (a,b,c),
    (a,b  ),
    (a    ),
    (     )
    )
    ```

`CUBE (a, b, c)` は、次の `GROUPING SETS` 句と同等です。

    ```sql
    GROUPING SETS (
    ( a, b, c ),
    ( a, b    ),
    ( a,    c ),
    ( a       ),
    (    b, c ),
    (    b    ),
    (       c ),
    (         )
    )
    ```

実際のデータセットでテストします。

    ```sql
    SELECT * FROM t;
    +------+------+------+
    | k1   | k2   | k3   |
    +------+------+------+
    | a    | A    |    1 |
    | a    | A    |    2 |
    | a    | B    |    1 |
    | a    | B    |    3 |
    | b    | A    |    1 |
    | b    | A    |    4 |
    | b    | B    |    1 |
    | b    | B    |    5 |
    +------+------+------+
    8 rows in set (0.01 sec)
  
    SELECT k1, k2, SUM(k3) FROM t
    GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
    +------+------+-----------+
    | k1   | k2   | sum(`k3`) |
    +------+------+-----------+
    | b    | B    |         6 |
    | a    | B    |         4 |
    | a    | A    |         3 |
    | b    | A    |         5 |
    | NULL | B    |        10 |
    | NULL | A    |         8 |
    | a    | NULL |         7 |
    | b    | NULL |        11 |
    | NULL | NULL |        18 |
    +------+------+-----------+
    9 rows in set (0.06 sec)
  
    SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t
    GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
    +------+------+---------------+----------------+
    | k1   | k2   | grouping_id(k1,k2) | sum(`k3`) |
    +------+------+---------------+----------------+
    | a    | A    |             0 |              3 |
    | a    | B    |             0 |              4 |
    | a    | NULL |             1 |              7 |
    | b    | A    |             0 |              5 |
    | b    | B    |             0 |              6 |
    | b    | NULL |             1 |             11 |
    | NULL | A    |             2 |              8 |
    | NULL | B    |             2 |             10 |
    | NULL | NULL |             3 |             18 |
    +------+------+---------------+----------------+
    9 rows in set (0.02 sec)
    ```

## HAVING

HAVING 句は、テーブル内の行データをフィルタリングするのではなく、集計関数の結果をフィルタリングします。

一般的に、HAVING は集計関数 (COUNT()、SUM()、AVG()、MIN()、MAX() など) および GROUP BY 句とともに使用されます。

**例**

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having sum(short_column) = 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|         2   |        1            |
+-------------+---------------------+

1 row in set (0.07 sec)
```

```sql
select tiny_column, sum(short_column) 
from small_table 
group by tiny_column 
having tiny_column > 1;
```

```plain text
+-------------+---------------------+
|tiny_column  | sum('short_column') |
+-------------+---------------------+
|      2      |          1          |
+-------------+---------------------+

1 row in set (0.07 sec)
```

## LIMIT

LIMIT 句は、返される行の最大数を制限するために使用されます。返される行の最大数を設定すると、StarRocks がメモリ使用量を最適化するのに役立ちます。

この句は主に次のシナリオで使用されます。

Top-N クエリの結果を返します。

以下のテーブルに含まれる内容を検討してください。

テーブル内のデータ量が多いため、または WHERE 句がデータをあまりフィルタリングしないため、クエリ結果セットのサイズを制限する必要があります。

使用上の注意：LIMIT 句の値は、数値リテラル定数でなければなりません。

**例**

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

## OFFSET

OFFSET 句を使用すると、結果セットは最初の数行をスキップし、後続の結果を直接返します。

結果セットはデフォルトで行 0 から始まるため、OFFSET 0 と OFFSET なしは同じ結果を返します。

一般的に、OFFSET 句は ORDER BY 句および LIMIT 句と組み合わせて使用する必要があります。

例：

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 3;

+----------------+
| varchar_column | 
+----------------+
|    beijing     | 
|    chongqing   | 
|    tianjin     | 
+----------------+

3 rows in set (0.02 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 0;

+----------------+
|varchar_column  |
+----------------+
|     beijing    |
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 1;

+----------------+
|varchar_column  |
+----------------+
|    chongqing   | 
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from big_table order by varchar_column limit 1 offset 2;

+----------------+
|varchar_column  |
+----------------+
|     tianjin    |     
+----------------+

1 row in set (0.02 sec)
```

注: order by なしで offset 構文を使用することは許可されていますが、この時点では offset は意味がありません。

この場合、limit 値のみが取得され、offset 値は無視されます。したがって、order by は不要です。

Offset が結果セットの最大行数を超えても、結果は表示されます。ユーザーは order by とともに offset を使用することをお勧めします。

## UNION

複数のクエリの結果を結合します。

**構文**

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

**パラメータ**

- `DISTINCT` (デフォルト): 一意の行のみを返します。UNION は UNION DISTINCT と同等です。
- `ALL`: 重複を含むすべての行を結合します。重複排除はメモリを大量に消費するため、UNION ALL を使用したクエリの方が高速で、メモリ消費量も少なくなります。パフォーマンスを向上させるには、UNION ALL を使用してください。

> **NOTE**
>
> 各クエリステートメントは、同じ数の列を返し、列は互換性のあるデータ型を持っている必要があります。

**例**

テーブル `select1` と `select2` を作成します。

```SQL
CREATE TABLE select1(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select1 VALUES
    (1,2),
    (1,2),
    (2,3),
    (5,6),
    (5,6);

CREATE TABLE select2(
    id          INT,
    price       INT
    )
DISTRIBUTED BY HASH(id);

INSERT INTO select2 VALUES
    (2,3),
    (3,4),
    (5,6),
    (7,8);
```

例1: 重複を含め、2つのテーブル内のすべてのIDを返します。

```Plaintext
mysql> (select id from select1) union all (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    1 |
|    2 |
|    2 |
|    3 |
|    5 |
|    5 |
|    5 |
|    7 |
+------+
11 rows in set (0.02 sec)
```

例 2：2 つのテーブルにある一意の ID をすべて返します。次の 2 つのステートメントは同等です。

```Plaintext
mysql> (select id from select1) union (select id from select2) order by id;

+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
6 rows in set (0.01 sec)

mysql> (select id from select1) union distinct (select id from select2) order by id;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
|    5 |
|    7 |
+------+
5 rows in set (0.02 sec)
```

例3：2つのテーブルにあるすべてのユニークなIDのうち、最初の3つのIDを返します。以下の2つのステートメントは同等です。

```SQL
mysql> (select id from select1) union distinct (select id from select2)
order by id
limit 3;
++------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
4 rows in set (0.11 sec)

mysql> select * from (select id from select1 union distinct select id from select2) as t1
order by id
limit 3;
+------+
| id   |
+------+
|    1 |
|    2 |
|    3 |
+------+
3 rows in set (0.01 sec)
```

## INTERSECT

複数のクエリの結果の共通部分、つまりすべての結果セットに現れる結果を計算します。この句は、結果セットの中から一意の行のみを返します。ALL キーワードはサポートされていません。

**構文**

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **NOTE**
>
> - INTERSECT は INTERSECT DISTINCT と同等です。
> - 各クエリステートメントは、同じ数のカラムを返し、カラムは互換性のあるデータ型を持っている必要があります。

**Examples**

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

## EXCEPT/MINUS

左側のクエリの結果のうち、右側のクエリに存在しない重複を除いた結果を返します。EXCEPT は MINUS と同等です。

**構文**

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **NOTE**
>
> - EXCEPT は EXCEPT DISTINCT と同等です。 ALL キーワードはサポートされていません。
> - 各クエリステートメントは、同じ数のカラムを返し、カラムは互換性のあるデータ型でなければなりません。

**Examples**

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

## DISTINCT

DISTINCT キーワードは、結果セットから重複する行を削除します。 例：

```SQL
-- Returns the unique values from one column.
select distinct tiny_column from big_table limit 2;

-- Returns the unique combinations of values from multiple columns.
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCT は、集計関数（通常はカウント関数）とともに使用でき、count (distinct) は、1つ以上のカラムに含まれる異なる組み合わせの数を計算するために使用されます。

```SQL
-- Counts the unique values from one column.
select count(distinct tiny_column) from small_table;
```

```plain text
+-------------------------------+
| count(DISTINCT 'tiny_column') |
+-------------------------------+
|             2                 |
+-------------------------------+
1 row in set (0.06 sec)
```

```SQL
-- Counts the unique combinations of values from multiple columns.
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks は、distinct を使用した複数の集計関数を同時にサポートしています。

```SQL
-- Count the unique value from multiple aggregation function separately.
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

## サブクエリ

サブクエリは、関連性に関して次の2つのタイプに分類されます。

- 非相関サブクエリ：外側のクエリとは独立して結果を取得します。
- 相関サブクエリ：外側のクエリからの値を必要とします。

#### 相関のないサブクエリ

相関のないサブクエリは、[NOT] IN と EXISTS をサポートしています。

**例**

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

v3.0 以降では、`SELECT... FROM... WHERE... [NOT] IN` の WHERE 句で複数のフィールドを指定できます。たとえば、2 番目の SELECT ステートメントの `WHERE (x,y)` などです。

#### 相関サブクエリ

相関サブクエリは、[NOT] IN と [NOT] EXISTS をサポートします。

**例**

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

サブクエリは、スカラー量子クエリもサポートしています。これは、無相関スカラー量子クエリ、相関スカラー量子クエリ、および一般関数のパラメータとしてのスカラー量子クエリに分類できます。

**例**

1. 述語が=符号の、無相関スカラー量子クエリ。たとえば、最も高い賃金を持つ人物に関する情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. 相関のないスカラー量子クエリ（述語`>`、`<`などを使用）。例えば、平均より高い給与を得ている人々の情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 関連するスカラー量子クエリ。たとえば、各部署の最高給与情報を出力します。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. スカラー量子クエリは、通常の関数のパラメーターとして使用されます。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

## Where と Operator

SQL operator は、比較に使用される一連の関数であり、select ステートメントの where 句で広く使用されています。

### 算術演算子

算術演算子は通常、左オペランド、右オペランド、そして多くの場合、左オペランドを含む式に現れます。

**+ と -**: 単項演算子または二項演算子として使用できます。単項演算子として使用する場合、+1、-2.5、-col_name のように、値に +1 または -1 を掛けることを意味します。

したがって、単項演算子 + は変更されていない値を返し、単項演算子 - はその値の符号ビットを変更します。

ユーザーは、+5 (正の値を返す)、-+2 または +-2 (負の値を返す) のように、2 つの単項演算子を重ねることができますが、2 つの連続する - 符号を使用することはできません。

なぜなら、-- は次のステートメントではコメントとして解釈されるからです (ユーザーが 2 つの - 符号を使用できる場合、2 つの - 符号の間にスペースまたは括弧が必要です。例えば、-(-2) または - -2 のように、実際には +2 になります)。

+ または - が二項演算子の場合、2+2、3+1.5、col1+col2 のように、左側の値に右側の値を加算または減算することを意味します。左右の値はどちらも数値型である必要があります。

**\* と /**: それぞれ乗算と除算を表します。両側のオペランドはデータ型である必要があります。2 つの数値を乗算する場合。

必要に応じて、より小さいオペランドが昇格される場合があります (例えば、SMALLINT から INT または BIGINT へ)。式の結果は、次に大きい型に昇格されます。

例えば、TINYINT に INT を掛けると、BIGINT 型の結果が生成されます。2 つの数値を乗算する場合、精度が失われるのを避けるために、オペランドと式の両方の結果は DOUBLE 型として解釈されます。

ユーザーが式の結果を別の型に変換したい場合は、CAST 関数を使用して変換する必要があります。

**%**: 剰余演算子。左オペランドを右オペランドで割った余りを返します。左右のオペランドはどちらも整数である必要があります。

**&、| と ^**: ビット単位演算子は、2 つのオペランドに対するビット単位 AND、ビット単位 OR、ビット単位 XOR 演算の結果を返します。両方のオペランドには整数型が必要です。

ビット単位演算子の 2 つのオペランドの型が一致しない場合、より小さい型のオペランドはより大きい型のオペランドに昇格され、対応するビット単位演算が実行されます。

複数の算術演算子を式に記述でき、ユーザーは対応する算術式を括弧で囲むことができます。算術演算子には通常、算術演算子と同じ機能を表現する対応する数学関数はありません。

例えば、% 演算子を表す MOD() 関数はありません。逆に、数学関数には対応する算術演算子はありません。例えば、べき乗関数 POW() には対応する ** 指数演算子はありません。

サポートされている算術関数は、数学関数セクションで確認できます。

### Between Operator

where 句では、式を上限と下限の両方と比較できます。式が下限以上で、上限以下の場合、比較の結果は true になります。

構文：

```sql
expression BETWEEN lower_bound AND upper_bound
```

データ型：通常、式は数値型に評価されますが、他のデータ型もサポートします。下限と上限の両方が比較可能な文字であることを保証する必要がある場合は、cast() 関数を使用できます。

 使用上の注意：オペランドが文字列型の場合、上限で始まる長い文字列は上限と一致しないことに注意してください。これは上限よりも大きいためです。たとえば、"between 'A' and 'M'" は 'MJ' と一致しません。

 式が正しく動作することを確認する必要がある場合は、upper()、lower()、substr()、trim() などの関数を使用できます。

 例：

```sql
select c1 from t1 where month between 1 and 6;
```

### 比較演算子

比較演算子は、2つの値を比較するために使用されます。`=`、`!=`、`>=` はすべてのデータ型に適用されます。

`<>` 演算子と `!=` 演算子は同等であり、2つの値が等しくないことを示します。

### In Operator

In operator は、VALUE コレクションと比較し、コレクション内のいずれかの要素と一致する場合に TRUE を返します。

パラメータと VALUE コレクションは比較可能である必要があります。IN operator を使用するすべての式は、OR で接続された同等の比較として記述できますが、IN の構文はよりシンプルで、より正確で、StarRocks が最適化しやすくなっています。

例：

```sql
select * from small_table where tiny_column in (1,2);
```

### Like Operator

この operator は、文字列との比較に使用されます。'\_'（アンダースコア）は単一の文字に一致し、'%' は複数の文字に一致します。パラメーターは文字列全体と一致する必要があります。通常、文字列の末尾に'%'を配置する方が実用的です。

例：

```plain text
mysql> select varchar_column from small_table where varchar_column like 'm%';

+----------------+
|varchar_column  |
+----------------+
|     milan      |
+----------------+

1 row in set (0.02 sec)
```

```plain
mysql> select varchar_column from small_table where varchar_column like 'm____';

+----------------+
| varchar_column | 
+----------------+
|    milan       | 
+----------------+

1 row in set (0.01 sec)
```

### 論理演算子

論理演算子は BOOL 値を返します。これには、単項演算子と多項演算子が含まれ、それぞれが BOOL 値を返す式であるパラメータを処理します。サポートされている演算子は次のとおりです。

AND: 2 項演算子。AND 演算子は、左右のパラメータが両方とも TRUE として計算される場合に TRUE を返します。

OR: 2 項演算子。左右のパラメータのいずれかが TRUE として計算される場合に TRUE を返します。両方のパラメータが FALSE の場合、OR 演算子は FALSE を返します。

NOT: 単項演算子。式を反転した結果を返します。パラメータが TRUE の場合、演算子は FALSE を返します。パラメータが FALSE の場合、演算子は TRUE を返します。

例：

```plain text
mysql> select true and true;

+-------------------+
| (TRUE) AND (TRUE) | 
+-------------------+
|         1         | 
+-------------------+

1 row in set (0.00 sec)
```

```plain text
mysql> select true and false;

+--------------------+
| (TRUE) AND (FALSE) | 
+--------------------+
|         0          | 
+--------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select true or false;

+-------------------+
| (TRUE) OR (FALSE) | 
+-------------------+
|        1          | 
+-------------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select not true;

+----------+
| NOT TRUE | 
+----------+
|     0    | 
+----------+

1 row in set (0.01 sec)
```

### 正規表現 Operator

正規表現が一致するかどうかを判断します。POSIX 標準の正規表現を使用し、'^' は文字列の先頭部分に一致し、'$' は文字列の末尾に一致します。

"." は任意の 1 文字に一致し、"*" は 0 個以上のオプションに一致し、"+" は 1 個以上のオプションに一致し、"?" は貪欲な表現を意味します。正規表現は、文字列の一部だけでなく、完全な値に一致する必要があります。

中間部分に一致させたい場合は、正規表現の前半部分を '^. ' または '.' と記述できます。'^' と '$' は通常省略されます。RLIKE operator と REGEXP operator は同義語です。

'|' operator はオプションの operator です。'|' の両側の正規表現は、片側の条件を満たすだけで済みます。'|' operator と両側の正規表現は、通常 () で囲む必要があります。

例：

```plain text
mysql> select varchar_column from small_table where varchar_column regexp '(mi|MI).*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |       
+----------------+

1 row in set (0.01 sec)
```

```plain text
mysql> select varchar_column from small_table where varchar_column regexp 'm.*';

+----------------+
| varchar_column | 
+----------------+
|     milan      |  
+----------------+

1 row in set (0.01 sec)
```

## エイリアス

クエリ内でテーブル、カラム、またはカラムを含む式の名前を記述する際に、エイリアスを割り当てることができます。エイリアスは通常、元の名前よりも短く、覚えやすいものです。

エイリアスが必要な場合は、SELECTリストまたはFROMリスト内のテーブル、カラム、および式の名前の後にAS句を追加するだけです。ASキーワードはオプションです。ASを使用せずに、元の名前の直後にエイリアスを指定することもできます。

エイリアスまたはその他の識別子が、内部の [StarRocks keyword](../keywords.md) と同じ名前を持つ場合、名前をバッククォートのペアで囲む必要があります。例：`rank`。

エイリアスは大文字と小文字を区別しますが、カラムエイリアスと式のエイリアスは大文字と小文字を区別しません。

例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

## PIVOT

この機能は v3.3 以降でサポートされています。

PIVOT 操作は SQL の高度な機能であり、テーブル内の行を列に変換できます。これは、ピボットテーブルの作成に特に役立ちます。これは、データベースレポートや分析を扱う場合、特にプレゼンテーション用にデータを要約または分類する必要がある場合に役立ちます。

実際、PIVOT はシンタックスシュガーであり、`sum(case when ... then ... end)` のようなクエリステートメントの記述を簡素化できます。

**構文**

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

**パラメータ**

PIVOT 操作では、いくつかの重要なコンポーネントを指定する必要があります。

- aggregate_function(): データの集計に使用される SUM、AVG、COUNT などの集計関数。
- alias: 集計結果のエイリアス。結果をより理解しやすくします。
- FOR pivot_column: 行から列への変換を実行する列名を指定します。
- IN (pivot_value): 列に変換される pivot_column の特定の値​​を指定します。

**例**

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

## EXCLUDE

この機能はバージョン 4.0 以降でサポートされています。

EXCLUDE キーワードは、クエリ結果から指定されたカラムを除外するために使用され、特定カラムを無視できる場合に SQL ステートメントを簡素化します。これは、多数のカラムを含むテーブルを操作する際に特に便利で、保持するすべてのカラムを明示的にリストする必要がなくなります。

**構文**

```sql  
SELECT  
  * EXCLUDE (<column_name> [, <column_name> ...])  
  | <table_alias>.* EXCLUDE (<column_name> [, <column_name> ...])  
FROM ...  
```

**パラメータ**

- **`* EXCLUDE`**  
  ワイルドカード `*` を使用してすべてのカラムを選択し、その後に `EXCLUDE` と除外するカラム名のリストを指定します。
- **`<table_alias>.* EXCLUDE`**  
  テーブルエイリアスが存在する場合、この構文を使用すると、そのテーブルから特定カラムを除外できます（エイリアスとともに使用する必要があります）。
- **`<column_name>`**  
  除外するカラム名。複数のカラムはカンマで区切ります。カラムはテーブルに存在する必要があります。存在しない場合、エラーが返されます。

**例**

- 基本的な使用例：

```sql  
-- Create test_table.
CREATE TABLE test_table (  
  id INT,  
  name VARCHAR(50),  
  age INT,  
  email VARCHAR(100)  
) DUPLICATE KEY(id);  

-- Exclude a single column (age).
SELECT * EXCLUDE (age) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, email FROM test_table;  

-- Exclude multiple columns (name, email).
SELECT * EXCLUDE (name, email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, age FROM test_table;  

-- Exclude columns using a table alias.
SELECT test_table.* EXCLUDE (email) FROM test_table;  
-- Above is equivalent to:  
SELECT id, name, age FROM test_table;  
```

## RECURSIVE

v4.1 以降、StarRocks は再帰的 Common Table Expression (CTE) をサポートし、反復的な実行アプローチを使用して、さまざまな階層型およびツリー構造化データを効率的に処理します。

再帰的 CTE は、それ自体を参照できる特殊なタイプの CTE であり、再帰クエリを可能にします。再帰的 CTE は、組織図、ファイルシステム、グラフ走査などの階層型データ構造の処理に特に役立ちます。

再帰的 CTE は、次のコンポーネントで構成されます。

- **アンカーメンバー**: 再帰的ではない初期クエリで、再帰の開始データセットを提供します。
- **再帰メンバー**: CTE 自体を参照する再帰クエリ。
- **終了条件**: 無限再帰を防ぐための条件。通常は WHERE 句を使用して実装されます。

再帰的 CTE の実行プロセスは次のとおりです。

1. アンカーメンバーを実行して、初期結果セット（レベル 0）を取得します。
2. レベル 0 の結果を入力として使用し、再帰メンバーを実行してレベル 1 の結果を取得します。
3. レベル 1 の結果を入力として使用し、再帰メンバーを再度実行してレベル 2 の結果を取得します。
4. 再帰メンバーが行を返さなくなるか、最大再帰深度に達するまで、このプロセスを繰り返します。
5. UNION ALL (または UNION) を使用して、すべてのレベルの結果をマージします。

:::tip
この機能を使用する前に、システム変数 `enable_recursive_cte` を `true` に設定して、この機能を有効にする必要があります。
:::

**構文**

```sql
WITH RECURSIVE cte_name [(column_list)] AS (
    -- Anchor member (non-recursive part)
    anchor_query
    UNION [ALL | DISTINCT]
    -- Recursive member (recursive part)
    recursive_query
)
SELECT ... FROM cte_name ...;
```

**パラメータ**

- `cte_name`: CTE の名前。
- `column_list` (オプション): CTE の結果セットの列名のリスト。
- `anchor_query`: 非再帰的で、CTE 自体を参照できない初期クエリ。
- `UNION`: Union operator。
  - `UNION ALL`: すべての行 (重複を含む) を保持します。パフォーマンス向上のため推奨されます。
  - `UNION` または `UNION DISTINCT`: 重複行を削除します。
- `recursive_query`: CTE 自体を参照する再帰クエリ。

**制限事項**

StarRocks の再帰 CTE には、次の制限があります。

- **フィーチャーフラグが必要**

  システム変数 `enable_recursive_cte` を `true` に設定して、再帰 CTE を手動で有効にする必要があります。

- **構造要件**
  - UNION または UNION ALL を使用して、アンカーメンバーと再帰メンバーを接続する必要があります。
  - アンカーメンバーは CTE 自体を参照できません。
  - 再帰メンバーが CTE 自体を参照しない場合、通常の CTE として実行されます。

- **再帰深度の制限**
  - デフォルトでは、再帰の最大深度は 5 (レベル) です。
  - 無限再帰を防ぐために、システム変数 `recursive_cte_max_depth` を使用して最大深度を調整できます。

- **実行制約**
  - 現在、複数レベルのネストされた再帰 CTE はサポートされていません。
  - 複雑な再帰 CTE は、パフォーマンスの低下につながる可能性があります。
  - `anchor_query` の定数は、`recursive_query` の出力タイプと一致するタイプである必要があります。

**構成**

再帰 CTE を使用するには、次のシステム変数が必要です。

| 変数名                      | タイプ   | デフォルト | 説明                                                         |
| -------------------------- | ------- | -------- | ------------------------------------------------------------ |
| `enable_recursive_cte`     | BOOLEAN | false    | 再帰 CTE を有効にするかどうか。                                  |
| `recursive_cte_max_depth`  | INT     | 5        | 無限再帰を防ぐための再帰の最大深度。                              |

**例**

**例 1: 組織階層のクエリ**

組織階層のクエリは、再帰 CTE の最も一般的なユースケースの 1 つです。次の例では、従業員の組織階層の関係をクエリします。

1. データの準備:

    ```sql
    CREATE TABLE employees (
      employee_id INT,
      name VARCHAR(100),
      manager_id INT,
      title VARCHAR(50)
    ) DUPLICATE KEY(employee_id)
    DISTRIBUTED BY RANDOM;

    INSERT INTO employees VALUES
    (1, 'Alicia', NULL, 'CEO'),
    (2, 'Bob', 1, 'CTO'),
    (3, 'Carol', 1, 'CFO'),
    (4, 'David', 2, 'VP of Engineering'),
    (5, 'Eve', 2, 'VP of Research'),
    (6, 'Frank', 3, 'VP of Finance'),
    (7, 'Grace', 4, 'Engineering Manager'),
    (8, 'Heidi', 4, 'Tech Lead'),
    (9, 'Ivan', 5, 'Research Manager'),
    (10, 'Judy', 7, 'Senior Engineer');
    ```

2. 組織階層のクエリ：

    ```sql
    WITH RECURSIVE org_hierarchy AS (
        -- Anchor member: Start from CEO (employee with no manager)
        SELECT 
            employee_id, 
            name, 
            manager_id, 
            title, 
            CAST(1 AS BIGINT) AS level,
            name AS path
        FROM employees
        WHERE manager_id IS NULL
        
        UNION ALL
        
        -- Recursive member: Find subordinates at next level
        SELECT 
            e.employee_id,
            e.name,
            e.manager_id,
            e.title,
            oh.level + 1,
            CONCAT(oh.path, ' -> ', e.name) AS path
        FROM employees e
        INNER JOIN org_hierarchy oh ON e.manager_id = oh.employee_id
    )
    SELECT /*+ SET_VAR(enable_recursive_cte=true) */
        employee_id,
        name,
        title,
        level,
        path
    FROM org_hierarchy
    ORDER BY employee_id;
    ```

結果：

```Plain
+-------------+---------+----------------------+-------+-----------------------------------------+
| employee_id | name    | title                | level | path                                    |
+-------------+---------+----------------------+-------+-----------------------------------------+
|           1 | Alicia  | CEO                  |     1 | Alicia                                  |
|           2 | Bob     | CTO                  |     2 | Alicia -> Bob                           |
|           3 | Carol   | CFO                  |     2 | Alicia -> Carol                         |
|           4 | David   | VP of Engineering    |     3 | Alicia -> Bob -> David                  |
|           5 | Eve     | VP of Research       |     3 | Alicia -> Bob -> Eve                    |
|           6 | Frank   | VP of Finance        |     3 | Alicia -> Carol -> Frank                |
|           7 | Grace   | Engineering Manager  |     4 | Alicia -> Bob -> David -> Grace         |
|           8 | Heidi   | Tech Lead            |     4 | Alicia -> Bob -> David -> Heidi         |
|           9 | Ivan    | Research Manager     |     4 | Alicia -> Bob -> Eve -> Ivan            |
|          10 | Judy    | Senior Engineer      |     5 | Alicia -> Bob -> David -> Grace -> Judy |
+-------------+---------+----------------------+-------+-----------------------------------------+
```

**例 2: 複数の再帰的 CTE**

1 つのクエリで複数の再帰的 CTE を定義できます。

```sql
WITH RECURSIVE
cte1 AS (
    SELECT CAST(1 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte1 WHERE n < 5
),
cte2 AS (
    SELECT CAST(10 AS BIGINT) AS n
    UNION ALL
    SELECT n + 1 FROM cte2 WHERE n < 15
)
SELECT /*+ SET_VAR(enable_recursive_cte=true) */
    'cte1' AS source,
    n
FROM cte1
UNION ALL
SELECT 
    'cte2' AS source,
    n
FROM cte2
ORDER BY source, n;
```
