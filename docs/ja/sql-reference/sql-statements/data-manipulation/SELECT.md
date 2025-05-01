---
displayed_sidebar: docs
---

# SELECT

## 説明

1 つ以上のテーブル、ビュー、またはマテリアライズドビューからデータをクエリします。SELECT 文は一般的に次の句で構成されます。

- [ WITH](#with)
- [ WHERE and operators](#where-and-operators)
- [ GROUP BY](#group-by)
- [ HAVING](#having)
- [ UNION](#union)
- [ INTERSECT](#intersect)
- [ EXCEPT/MINUS](#exceptminus)
- [ ORDER BY](#order-by)
- [ LIMIT](#limit)
- [ OFFSET](#offset)
- [ Joins](#join)
- [ Subqueries](#subquery)
- [ DISTINCT](#distinct)
- [ Alias](#alias)

SELECT は独立した文として、または他の文にネストされた句として機能します。SELECT 句の出力は、他の文の入力として使用できます。

StarRocks のクエリ文は基本的に SQL92 標準に準拠しています。ここでは、サポートされている SELECT の使用法について簡単に説明します。

### WITH

SELECT 文の前に追加できる句で、SELECT 内で複数回参照される複雑な式にエイリアスを定義します。

CRATE VIEW に似ていますが、句で定義されたテーブル名と列名はクエリ終了後に保持されず、実際のテーブルや VIEW の名前と競合しません。

WITH 句を使用する利点は次のとおりです。

クエリ内の重複を減らし、便利でメンテナンスが容易です。

クエリの最も複雑な部分を別々のブロックに抽象化することで、SQL コードを読みやすく理解しやすくします。

例:

```sql
-- UNION ALL クエリの初期段階として、外部レベルで 1 つのサブクエリを定義し、
-- 内部レベルで別のサブクエリを定義します。

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

### Join

ジョイン操作は、2 つ以上のテーブルからデータを結合し、それらの一部の列の結果セットを返します。

StarRocks は、自己ジョイン、クロスジョイン、内部ジョイン、外部ジョイン、セミジョイン、およびアンチジョインをサポートしています。外部ジョインには、左ジョイン、右ジョイン、および完全ジョインが含まれます。

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

#### Self Join

StarRocks は自己ジョインをサポートしており、これは自己ジョインと自己ジョインです。たとえば、同じテーブルの異なる列が結合されます。

実際には、自己ジョインを識別する特別な構文はありません。自己ジョインのジョイン条件の両側の条件は同じテーブルから来ます。

それらに異なるエイリアスを割り当てる必要があります。

例:

```sql
SELECT lhs.id, rhs.parent, lhs.c1, rhs.c2 FROM tree_data lhs, tree_data rhs WHERE lhs.id = rhs.parent;
```

#### Cross Join

クロスジョインは多くの結果を生成する可能性があるため、クロスジョインは注意して使用する必要があります。

クロスジョインを使用する必要がある場合でも、フィルター条件を使用し、返される結果が少ないことを確認する必要があります。例:

```sql
SELECT * FROM t1, t2;

SELECT * FROM t1 CROSS JOIN t2;
```

#### Inner Join

内部ジョインは最もよく知られ、一般的に使用されるジョインです。両方のテーブルの列が同じ値を含む場合に結合される、2 つの類似したテーブルから要求された列の結果を返します。

両方のテーブルの列名が同じ場合、完全な名前 (table_name.column_name の形式) を使用するか、列名にエイリアスを付ける必要があります。

例:

次の 3 つのクエリは同等です。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

#### Outer Join

外部ジョインは、左または右のテーブル、または両方のすべての行を返します。別のテーブルに一致するデータがない場合は、NULL に設定します。例:

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### 同等および不等ジョイン

通常、ユーザーは最も等しいジョインを使用し、ジョイン条件の演算子が等号である必要があります。

不等ジョインは、ジョイン条件に!=、等号を使用できます。不等ジョインは多くの結果を生成し、計算中にメモリ制限を超える可能性があります。

注意して使用してください。不等ジョインは内部ジョインのみをサポートします。例:

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

#### Semi Join

左セミジョインは、右テーブルのデータと一致する左テーブルの行のみを返します。右テーブルのデータと一致する行数に関係なく、この左テーブルの行は最大 1 回返されます。右セミジョインは同様に機能しますが、返されるデータは右テーブルです。

例:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT SEMI JOIN t2 ON t1.id = t2.id;
```

#### Anti Join

左アンチジョインは、右テーブルと一致しない左テーブルの行のみを返します。

右アンチジョインはこの比較を逆にし、左テーブルと一致しない右テーブルの行のみを返します。例:

```sql
SELECT t1.c1, t1.c2, t1.c2 FROM t1 LEFT ANTI JOIN t2 ON t1.id = t2.id;
```

#### 同等ジョインと非同等ジョイン

StarRocks がサポートするさまざまなジョインは、ジョインで指定されたジョイン条件に応じて、同等ジョインと非同等ジョインに分類できます。

| **Equi****-joins**         | 自己ジョイン、クロスジョイン、内部ジョイン、外部ジョイン、セミジョイン、およびアンチジョイン |
| -------------------------- | ------------------------------------------------------------ |
| **Non-****equi****-joins** | クロスジョイン、内部ジョイン、左セミジョイン、左アンチジョイン、および外部ジョイン   |

- Equi-joins
  
  同等ジョインは、2 つのジョイン項目が `=` 演算子で結合されるジョイン条件を使用します。例: `a JOIN b ON a.id = b.id`.

- Non-equi-joins
  
  非同等ジョインは、2 つのジョイン項目が `<`, `<=`, `>`, `>=`, または `<>` などの比較演算子で結合されるジョイン条件を使用します。例: `a JOIN b ON a.id < b.id`. 非同等ジョインは同等ジョインよりも遅く実行されます。非同等ジョインを使用する場合は注意をお勧めします。

  次の 2 つの例は、非同等ジョインの実行方法を示しています:

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

### ORDER BY

SELECT 文の ORDER BY 句は、1 つ以上の列の値を比較して結果セットをソートします。

ORDER BY は時間とリソースを消費する操作です。すべての結果を 1 つのノードに送信してマージする必要があり、その後に結果をソートできます。ソートは、ORDER BY のないクエリよりも多くのメモリリソースを消費します。

したがって、ソートされた結果セットから最初の `N` 結果のみが必要な場合は、LIMIT 句を使用してメモリ使用量とネットワークオーバーヘッドを削減できます。LIMIT 句が指定されていない場合、デフォルトで最初の 65535 結果が返されます。

構文:

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

`ASC` は、結果を昇順で返すことを指定します。`DESC` は、結果を降順で返すことを指定します。順序が指定されていない場合、デフォルトは ASC (昇順) です。例:

```sql
select * from big_table order by tiny_column, short_column desc;
```

NULL 値のソート順: `NULLS FIRST` は、NULL 値を非 NULL 値の前に返すことを示します。`NULLS LAST` は、NULL 値を非 NULL 値の後に返すことを示します。

例:

```sql
select  *  from  sales_record  order by  employee_id  nulls first;
```

### GROUP BY

GROUP BY 句は、COUNT(), SUM(), AVG(), MIN(), MAX() などの集計関数と一緒に使用されることがよくあります。

GROUP BY で指定された列は集計操作に参加しません。GROUP BY 句には、集計関数によって生成された結果をフィルタリングするために Having 句を追加できます。

例:

```sql
select tiny_column, sum(short_column)
from small_table 
group by tiny_column;
```

```plain text
+-------------+---------------------+
| tiny_column |  sum('short_column')|
+-------------+---------------------+
|      1      |        2            |
|      2      |        1            |
+-------------+---------------------+
```

#### 構文

  ```sql
  SELECT ...
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

#### パラメータ

  `groupSet` は、選択リスト内の列、エイリアス、または式で構成されるセットを表します。`groupSet ::= { ( expr  [ , expr [ , ... ] ] )}`

  `expr` は、選択リスト内の列、エイリアス、または式を示します。

#### 注意

StarRocks は PostgreSQL のような構文をサポートしています。構文の例は次のとおりです:

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
  ```

  `ROLLUP(a,b,c)` は次の `GROUPING SETS` 文と同等です:

  ```sql
  GROUPING SETS (
  (a,b,c),
  ( a, b ),
  ( a),
  ( )
  )
  ```

  `CUBE ( a, b, c )` は次の `GROUPING SETS` 文と同等です:

  ```sql
  GROUPING SETS (
  ( a, b, c ),
  ( a, b ),
  ( a,    c ),
  ( a       ),
  (    b, c ),
  (    b    ),
  (       c ),
  (         )
  )
  ```

#### 例

  以下は実際のデータの例です:

  ```plain text
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

  SELECT k1, k2, SUM(k3) FROM t GROUP BY GROUPING SETS ( (k1, k2), (k2), (k1), ( ) );
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

  > SELECT k1, k2, GROUPING_ID(k1,k2), SUM(k3) FROM t GROUP BY GROUPING SETS ((k1, k2), (k1), (k2), ());
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

GROUP BY `GROUPING SETS` ｜ `CUBE` ｜ `ROLLUP` は GROUP BY 句の拡張です。GROUP BY 句内の複数のセットのグループ化を実現できます。結果は、複数の対応する GROUP BY 句の UNION 操作と同等です。

GROUP BY 句は、1 つの要素のみを含む GROUP BY GROUPING SETS の特別なケースです。たとえば、GROUPING SETS 文:

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  ```

  クエリ結果は次のように等価です:

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
  UNION
  SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
  UNION
  SELECT null, b, SUM( c ) FROM tab1 GROUP BY b
  UNION
  SELECT null, null, SUM( c ) FROM tab1
  ```

  `GROUPING(expr)` は、列が集計列であるかどうかを示します。集計列である場合は 0、そうでない場合は 1 です。

`GROUPING_ID(expr  [ , expr [ , ... ] ])` は GROUPING に似ています。GROUPING_ID は、指定された列の順序に従って列リストのビットマップ値を計算し、各ビットは GROUPING の値です。

GROUPING_ID() 関数はビットベクトルの 10 進値を返します。

### HAVING

HAVING 句は、テーブル内の行データをフィルタリングするのではなく、集計関数の結果をフィルタリングします。

一般的に、HAVING は集計関数 (COUNT(), SUM(), AVG(), MIN(), MAX() など) および GROUP BY 句と一緒に使用されます。

例:

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

### LIMIT

LIMIT 句は、返される行の最大数を制限するために使用されます。返される行の最大数を設定することで、StarRocks のメモリ使用量を最適化するのに役立ちます。

この句は主に次のシナリオで使用されます:

トップ N クエリの結果を返します。

以下の表に含まれる内容を考えてみてください。

テーブル内のデータ量が多いため、または where 句があまり多くのデータをフィルタリングしないため、クエリ結果セットのサイズを制限する必要があります。

使用方法: LIMIT 句の値は数値リテラル定数でなければなりません。

例:

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

### OFFSET

OFFSET 句は、結果セットが最初の数行をスキップし、その後の結果を直接返すようにします。

結果セットはデフォルトで行 0 から始まるため、offset 0 と offset なしは同じ結果を返します。

一般的に、OFFSET 句は ORDER BY および LIMIT 句と一緒に使用する必要があります。

例:

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

注意: order by なしで offset 構文を使用することは許可されていますが、この場合 offset は意味を持ちません。

この場合、limit 値のみが取得され、offset 値は無視されます。したがって、order by なしでは意味がありません。

結果セットの最大行数を超えるオフセットがあっても結果は変わりません。ユーザーには、order by と一緒に offset を使用することをお勧めします。

### UNION

複数のクエリの結果を結合します。

**構文:**

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

- DISTINCT (デフォルト) は一意の行のみを返します。UNION は UNION DISTINCT と同等です。
- ALL は重複を含むすべての行を結合します。重複排除はメモリを多く消費するため、UNION ALL を使用したクエリは高速でメモリ消費が少なくなります。より良いパフォーマンスのために、UNION ALL を使用してください。

> **注意**
>
> 各クエリ文は同じ数の列を返す必要があり、列は互換性のあるデータ型を持っている必要があります。

**例:**

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

例 1: 重複を含む 2 つのテーブル内のすべての ID を返します。

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

例 2: 2 つのテーブル内のすべての一意の ID を返します。次の 2 つの文は同等です。

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

例 3: 2 つのテーブル内のすべての一意の ID のうち、最初の 3 つの ID を返します。次の 2 つの文は同等です。

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

### **INTERSECT**

複数のクエリの結果の交差を計算します。つまり、すべての結果セットに現れる結果を返します。この句は、結果セットの中で一意の行のみを返します。ALL キーワードはサポートされていません。

**構文:**

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **注意**
>
> - INTERSECT は INTERSECT DISTINCT と同等です。
> - 各クエリ文は同じ数の列を返す必要があり、列は互換性のあるデータ型を持っている必要があります。

**例:**

UNION で使用した 2 つのテーブルを使用します。

2 つのテーブルに共通する一意の `(id, price)` の組み合わせを返します。次の 2 つの文は同等です。

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

### **EXCEPT/MINUS**

左側のクエリの一意の結果を返し、右側のクエリには存在しないものを返します。EXCEPT は MINUS と同等です。

**構文:**

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **注意**
>
> - EXCEPT は EXCEPT DISTINCT と同等です。ALL キーワードはサポートされていません。
> - 各クエリ文は同じ数の列を返す必要があり、列は互換性のあるデータ型を持っている必要があります。

**例:**

UNION で使用した 2 つのテーブルを使用します。

`select1` にあり、`select2` に存在しない一意の `(id, price)` の組み合わせを返します。

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

### DISTINCT

DISTINCT キーワードは結果セットを重複排除します。例:

```SQL
-- 1 列の一意の値を返します。
select distinct tiny_column from big_table limit 2;

-- 複数の列の値の一意の組み合わせを返します。
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCT は集計関数 (通常は count 関数) と一緒に使用でき、count (distinct) は 1 つ以上の列に含まれる異なる組み合わせの数を計算するために使用されます。

```SQL
-- 1 列の一意の値をカウントします。
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
-- 複数の列の値の一意の組み合わせをカウントします。
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocks は、同時に distinct を使用する複数の集計関数をサポートしています。

```SQL
-- 複数の集計関数から一意の値を個別にカウントします。
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

### Subquery

サブクエリは、関連性に基づいて 2 つのタイプに分類されます:

- 非相関サブクエリは、外部クエリとは独立して結果を取得します。
- 相関サブクエリは、外部クエリからの値を必要とします。

#### 非相関サブクエリ

非相関サブクエリは [NOT] IN および EXISTS をサポートします。

例:

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);
```

```sql
SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

#### 相関サブクエリ

関連サブクエリは [NOT] IN および [NOT] EXISTS をサポートします。

例:

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

サブクエリはスカラー量子クエリもサポートしています。これは、無関係なスカラー量子クエリ、関連するスカラー量子クエリ、一般的な関数のパラメータとしてのスカラー量子クエリに分けられます。

例:

1. = 記号を持つ無関係なスカラー量子クエリ。たとえば、最高賃金の人物に関する情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. `>`, `<` などの述語を持つ無関係なスカラー量子クエリ。たとえば、平均よりも多く支払われている人々に関する情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 関連するスカラー量子クエリ。たとえば、各部門の最高給与情報を出力します。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. スカラー量子クエリが通常の関数のパラメータとして使用されます。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

### Where and Operators

SQL 演算子は、比較に使用される一連の関数であり、SELECT 文の WHERE 句で広く使用されます。

#### 算術演算子

算術演算子は通常、左、右、およびほとんどの場合、左オペランドを含む式に現れます。

**+および-**: 単項演算子または 2 項演算子として使用できます。単項演算子として使用される場合、たとえば +1、-2.5、または -col_name は、値が +1 または -1 で乗算されることを意味します。

したがって、セル演算子 + は変更されていない値を返し、セル演算子 - はその値の符号ビットを変更します。

ユーザーは 2 つのセル演算子を重ねることができます。たとえば、+5 (正の値を返す)、-+2 または +2 (負の値を返す) ですが、ユーザーは 2 つの連続した - 記号を使用することはできません。

なぜなら、-- は次の文でコメントとして解釈されるためです (ユーザーが 2 記号を使用できる場合、2 記号の間にスペースまたは括弧が必要です。たとえば、-(-2) または - -2 は実際には +2 になります)。

+ または - が 2 項演算子である場合、たとえば 2+2、3+1.5、または col1+col2 は、左の値が右の値に加算または減算されることを意味します。左と右の値は両方とも数値型でなければなりません。

**および/**: それぞれ乗算と除算を表します。両側のオペランドはデータ型でなければなりません。2 つの数値が乗算されるとき。

必要に応じて小さいオペランドが昇格される場合があり (たとえば、SMALLINT から INT または BIGINT への昇格)、式の結果は次の大きい型に昇格されます。

たとえば、TINYINT を INT で乗算すると、BIGINT 型の結果が生成されます。2 つの数値が乗算されるとき、オペランドと式の結果は精度の損失を避けるために DOUBLE 型として解釈されます。

ユーザーが式の結果を別の型に変換したい場合は、CAST 関数を使用して変換する必要があります。

**%**: 剰余演算子。左オペランドを右オペランドで割った余りを返します。左と右のオペランドは両方とも整数でなければなりません。

**&, | および ^**: ビット演算子は、2 つのオペランドに対してビット単位の AND、ビット単位の OR、ビット単位の XOR 操作の結果を返します。両方のオペランドは整数型である必要があります。

ビット演算子の 2 つのオペランドの型が一致しない場合、小さい型のオペランドは大きい型のオペランドに昇格され、対応するビット単位の操作が実行されます。

式には複数の算術演算子が現れることがあり、ユーザーは対応する算術式を括弧で囲むことができます。算術演算子には、算術演算子と同じ機能を表す対応する数学関数が通常ありません。

たとえば、% 演算子を表す MOD() 関数はありません。逆に、数学関数には対応する算術演算子がありません。たとえば、べき乗関数 POW() には対応する ** べき乗演算子がありません。

ユーザーは、サポートされている算術関数を Mathematical Functions セクションで確認できます。

#### Between Operator

where 句では、式を上限と下限の両方と比較することができます。式が下限以上であり、上限以下である場合、比較の結果は true です。

構文:

```sql
expression BETWEEN lower_bound AND upper_bound
```

データ型: 通常、式は数値型に評価されますが、他のデータ型もサポートしています。下限と上限の両方が比較可能な文字であることを保証する必要がある場合は、cast() 関数を使用できます。

使用方法: オペランドが文字列型の場合、上限で始まる長い文字列は上限と一致しないことに注意してください。これは上限よりも大きいです。たとえば、"between 'A' and 'M'" は 'MJ' と一致しません。

式が正しく機能することを確認する必要がある場合は、upper()、lower()、substr()、trim() などの関数を使用できます。

例:

```sql
select c1 from t1 where month between 1 and 6;
```

#### 比較演算子

比較演算子は、2 つの値を比較するために使用されます。`=`, `!=`, `>=` はすべてのデータ型に適用されます。

`<>` と `!=` 演算子は同等であり、2 つの値が等しくないことを示します。

#### In Operator

In 演算子は、値のコレクションと比較し、コレクション内のいずれかの要素と一致する場合に TRUE を返します。

パラメータと値のコレクションは比較可能でなければなりません。IN 演算子を使用するすべての式は、OR で接続された同等の比較として記述できますが、IN の構文はより簡単で正確であり、StarRocks が最適化しやすくなります。

例:

```sql
select * from small_table where tiny_column in (1,2);
```

#### Like Operator

この演算子は文字列と比較するために使用されます。'_' は単一の文字に一致し、'%' は複数の文字に一致します。パラメータは完全な文字列と一致する必要があります。通常、文字列の末尾に '%' を置く方が実用的です。

例:

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

#### 論理演算子

論理演算子は BOOL 値を返し、単項および複数の演算子を含みます。各演算子は BOOL 値を返す式を処理します。サポートされている演算子は次のとおりです:

AND: 2 項演算子で、左と右のパラメータが両方とも TRUE と評価される場合、AND 演算子は TRUE を返します。

OR: 2 項演算子で、左と右のパラメータのいずれかが TRUE と評価される場合、OR 演算子は TRUE を返します。両方のパラメータが FALSE の場合、OR 演算子は FALSE を返します。

NOT: 単項演算子で、式の結果を反転させます。パラメータが TRUE の場合、演算子は FALSE を返します。パラメータが FALSE の場合、演算子は TRUE を返します。

例:

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

#### 正規表現演算子

正規表現が一致するかどうかを判断します。POSIX 標準の正規表現を使用し、'^' は文字列の最初の部分に一致し、'$' は文字列の終わりに一致します。

"." は任意の単一の文字に一致し、"*" は 0 回以上のオプションに一致し、"+" は 1 回以上のオプションに一致し、"?" は貪欲な表現を意味します。正規表現は、文字列の一部ではなく、完全な値に一致する必要があります。

中間部分に一致させたい場合、正規表現の前の部分は '^.' または '.' として書くことができます。通常、'^' と '$' は省略されます。RLIKE 演算子と REGEXP 演算子は同義です。

'|' 演算子はオプションの演算子です。'|' の両側の正規表現は、片方の条件を満たすだけでよいです。'|' 演算子とその両側の正規表現は通常 () で囲む必要があります。

例:

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

### Alias

クエリ内でテーブル、列、または列を含む式の名前を記述するときに、それらにエイリアスを割り当てることができます。エイリアスは通常、元の名前よりも短く、覚えやすいです。

エイリアスが必要な場合、選択リストまたは from リスト内のテーブル、列、式の名前の後に単に AS 句を追加するだけです。AS キーワードはオプションです。AS を使用せずに、元の名前の直後にエイリアスを指定することもできます。

エイリアスまたはその他の識別子が内部の [StarRocks キーワード](../keywords.md) と同じ名前を持つ場合、名前をバッククォートで囲む必要があります。たとえば、`rank` です。

エイリアスは大文字と小文字を区別しますが、列エイリアスと式エイリアスは大文字と小文字を区別しません。

例:

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```