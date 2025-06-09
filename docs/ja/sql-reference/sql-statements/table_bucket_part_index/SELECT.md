---
displayed_sidebar: docs
---

# SELECT

## 説明

1つ以上のテーブル、ビュー、またはマテリアライズドビューからデータをクエリします。SELECT文は一般的に以下の句で構成されます。

- [WITH](#with)
- [WHEREと演算子](#where-and-operators)
- [GROUP BY](#group-by)
- [HAVING](#having)
- [UNION](#union)
- [INTERSECT](#intersect)
- [EXCEPT/MINUS](#exceptminus)
- [ORDER BY](#order-by)
- [LIMIT](#limit)
- [OFFSET](#offset)
- [Joins](#join)
- [サブクエリ](#subquery)
- [DISTINCT](#distinct)
- [エイリアス](#alias)

SELECTは独立した文としても、他の文にネストされた句としても機能します。SELECT句の出力は他の文の入力として使用できます。

StarRocksのクエリ文は基本的にSQL92標準に準拠しています。ここではサポートされているSELECTの使用法について簡単に説明します。

> **NOTE**
>
> StarRocks内部テーブルのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、これらのオブジェクトに対するSELECT権限が必要です。外部データソースのテーブル、ビュー、またはマテリアライズドビューからデータをクエリするには、対応するexternal catalogに対するUSAGE権限が必要です。

### WITH

SELECT文の前に追加できる句で、SELECT内で複数回参照される複雑な式にエイリアスを定義します。

CRATE VIEWに似ていますが、句で定義されたテーブル名とカラム名はクエリ終了後に保持されず、実際のテーブルやVIEWの名前と競合しません。

WITH句を使用する利点は以下の通りです。

クエリ内の重複を減らし、便利でメンテナンスが容易です。

クエリの最も複雑な部分を別々のブロックに抽象化することで、SQLコードを読みやすく理解しやすくします。

例:

```sql
-- 外部レベルで1つのサブクエリを定義し、UNION ALLクエリの初期段階の一部として
-- 内部レベルで別のサブクエリを定義します。

with t1 as (select 1),t2 as (select 2)
select * from t1 union all select * from t2;
```

### Join

ジョイン操作は、2つ以上のテーブルからデータを結合し、その中のいくつかのカラムの結果セットを返します。

StarRocksは、自己ジョイン、クロスジョイン、内部ジョイン、外部ジョイン、セミジョイン、アンチジョインをサポートしています。外部ジョインには、左ジョイン、右ジョイン、フルジョインが含まれます。

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

StarRocksは自己ジョインをサポートしており、自己ジョインと自己ジョインです。例えば、同じテーブルの異なるカラムを結合します。

自己ジョインを識別する特別な構文は実際にはありません。自己ジョインのジョイン条件の両側は同じテーブルから来ます。

異なるエイリアスを割り当てる必要があります。

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

内部ジョインは最もよく知られ、一般的に使用されるジョインです。2つの類似したテーブルの要求されたカラムから結果を返し、両方のテーブルのカラムが同じ値を含む場合に結合されます。

両方のテーブルのカラム名が同じ場合、完全な名前（table_name.column_nameの形式）を使用するか、カラム名にエイリアスを付ける必要があります。

例:

以下の3つのクエリは同等です。

```sql
SELECT t1.id, c1, c2 FROM t1, t2 WHERE t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;
```

#### Outer Join

外部ジョインは、左または右のテーブル、または両方のすべての行を返します。他のテーブルに一致するデータがない場合は、NULLに設定されます。例:

```sql
SELECT * FROM t1 LEFT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 RIGHT OUTER JOIN t2 ON t1.id = t2.id;

SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### 同等ジョインと不等ジョイン

通常、ユーザーは最も等しいジョインを使用し、ジョイン条件の演算子が等号であることを要求します。

不等ジョインは、ジョイン条件で!=、等号を使用できます。不等ジョインは多くの結果を生成し、計算中にメモリ制限を超える可能性があります。

注意して使用してください。不等ジョインは内部ジョインのみをサポートします。例:

```sql
SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id = t2.id;

SELECT t1.id, c1, c2 FROM t1 INNER JOIN t2 ON t1.id > t2.id;
```

#### Semi Join

左セミジョインは、右テーブルのデータと一致する左テーブルの行のみを返します。右テーブルのデータと一致する行がいくつあっても関係ありません。

左テーブルのこの行は最大1回返されます。右セミジョインは同様に機能しますが、返されるデータは右テーブルです。

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

#### Equi-joinとNon-equi-join

StarRocksがサポートするさまざまなジョインは、指定されたジョイン条件に応じて、等価ジョインと非等価ジョインに分類されます。

| **Equi-joins**         | 自己ジョイン、クロスジョイン、内部ジョイン、外部ジョイン、セミジョイン、アンチジョイン |
| -------------------------- | ------------------------------------------------------------ |
| **Non-equi-joins** | クロスジョイン、内部ジョイン、左セミジョイン、左アンチジョイン、外部ジョイン   |

- Equi-joins
  
  等価ジョインは、`=` 演算子で2つのジョイン項目を結合するジョイン条件を使用します。例: `a JOIN b ON a.id = b.id`.

- Non-equi-joins
  
  非等価ジョインは、`<`, `<=`, `>`, `>=`, または `<>` などの比較演算子で2つのジョイン項目を結合するジョイン条件を使用します。例: `a JOIN b ON a.id < b.id`. 非等価ジョインは等価ジョインよりも遅く実行されます。非等価ジョインを使用する際は注意をお勧めします。

  以下の2つの例は、非等価ジョインを実行する方法を示しています。

  ```SQL
  SELECT t1.id, c1, c2 
  FROM t1 
  INNER JOIN t2 ON t1.id < t2.id;
    
  SELECT t1.id, c1, c2 
  FROM t1 
  LEFT JOIN t2 ON t1.id > t2.id;
  ```

### ORDER BY

SELECT文のORDER BY句は、1つ以上のカラムの値を比較して結果セットをソートします。

ORDER BYは時間とリソースを消費する操作です。すべての結果を1つのノードに送信してマージしてからソートする必要があるためです。ソートはORDER BYのないクエリよりも多くのメモリリソースを消費します。

したがって、ソートされた結果セットから最初の`N`個の結果のみが必要な場合は、LIMIT句を使用してメモリ使用量とネットワークオーバーヘッドを削減できます。LIMIT句が指定されていない場合、デフォルトで最初の65535個の結果が返されます。

構文:

```sql
ORDER BY <column_name> 
    [ASC | DESC]
    [NULLS FIRST | NULLS LAST]
```

`ASC`は結果を昇順で返すことを指定します。`DESC`は結果を降順で返すことを指定します。順序が指定されていない場合、デフォルトはASC（昇順）です。例:

```sql
select * from big_table order by tiny_column, short_column desc;
```

NULL値のソート順: `NULLS FIRST`はNULL値が非NULL値の前に返されることを示します。`NULLS LAST`はNULL値が非NULL値の後に返されることを示します。

例:

```sql
select  *  from  sales_record  order by  employee_id  nulls first;
```

### GROUP BY

GROUP BY句は、COUNT()、SUM()、AVG()、MIN()、MAX()などの集計関数とよく一緒に使用されます。

GROUP BYで指定されたカラムは集計操作に参加しません。GROUP BY句は、集計関数によって生成された結果をフィルタリングするためにHaving句と一緒に追加できます。

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

  `groupSet`は、selectリスト内のカラム、エイリアス、または式で構成されるセットを表します。  `groupSet ::= { ( expr  [ , expr [ , ... ] ] )}`

  `expr`は、selectリスト内のカラム、エイリアス、または式を示します。

#### 注意

StarRocksはPostgreSQLのような構文をサポートしています。構文の例は以下の通りです。

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY ROLLUP(a,b,c)
  SELECT a, b,c, SUM( d ) FROM tab1 GROUP BY CUBE(a,b,c)
  ```

  `ROLLUP(a,b,c)`は、以下の`GROUPING SETS`文と同等です。

  ```sql
  GROUPING SETS (
  (a,b,c),
  ( a, b ),
  ( a),
  ( )
  )
  ```

  `CUBE ( a, b, c )` は、以下の`GROUPING SETS`文と同等です。

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

  以下は実際のデータの例です。

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

GROUP BY `GROUPING SETS` ｜ `CUBE` ｜ `ROLLUP` は、GROUP BY句の拡張です。GROUP BY句内の複数のセットのグループ化を実現できます。結果は、複数の対応するGROUP BY句のUNION操作と同等です。

GROUP BY句は、1つの要素のみを含むGROUP BY GROUPING SETSの特別なケースです。例えば、GROUPING SETS文:

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY GROUPING SETS ( (a, b), (a), (b), ( ) );
  ```

  クエリ結果は以下と同等です。

  ```sql
  SELECT a, b, SUM( c ) FROM tab1 GROUP BY a, b
  UNION
  SELECT a, null, SUM( c ) FROM tab1 GROUP BY a
  UNION
  SELECT null, b, SUM( c ) FROM tab1 GROUP BY b
  UNION
  SELECT null, null, SUM( c ) FROM tab1
  ```

`GROUPING(expr)` は、カラムが集計カラムであるかどうかを示します。集計カラムである場合は0、そうでない場合は1です。

`GROUPING_ID(expr  [ , expr [ , ... ] ])` はGROUPINGに似ています。GROUPING_IDは、指定されたカラム順にカラムリストのビットマップ値を計算し、各ビットはGROUPINGの値です。

GROUPING_ID() 関数はビットベクトルの10進値を返します。

### HAVING

HAVING句はテーブル内の行データをフィルタリングするのではなく、集計関数の結果をフィルタリングします。

一般的に、HAVINGは集計関数（COUNT()、SUM()、AVG()、MIN()、MAX()など）およびGROUP BY句と一緒に使用されます。

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

LIMIT句は、返される行の最大数を制限するために使用されます。返される行の最大数を設定することで、StarRocksのメモリ使用量を最適化するのに役立ちます。

この句は主に以下のシナリオで使用されます。

トップNクエリの結果を返します。

以下のテーブルに含まれるものを考えてみてください。

テーブル内のデータ量が多いため、またはwhere句があまりデータをフィルタリングしないため、クエリ結果セットのサイズを制限する必要があります。

使用方法: LIMIT句の値は数値リテラル定数でなければなりません。

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

OFFSET句は、結果セットが最初の数行をスキップし、その後の結果を直接返すようにします。

結果セットはデフォルトで行0から始まるため、offset 0とoffsetなしは同じ結果を返します。

一般的に、OFFSET句はORDER BYおよびLIMIT句と一緒に使用する必要があります。

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

注意: order byなしでoffset構文を使用することは許可されていますが、この場合offsetは意味を持ちません。

この場合、limit値のみが考慮され、offset値は無視されます。したがって、order byなしでは。

結果セットの最大行数を超えるoffsetは依然として結果です。ユーザーがoffsetをorder byと一緒に使用することをお勧めします。

### UNION

複数のクエリの結果を結合します。

**構文:**

```sql
query_1 UNION [DISTINCT | ALL] query_2
```

- DISTINCT（デフォルト）は一意の行のみを返します。UNIONはUNION DISTINCTと同等です。
- ALLは重複を含むすべての行を結合します。重複排除はメモリを多く消費するため、UNION ALLを使用するクエリは高速でメモリ消費が少なくなります。より良いパフォーマンスのために、UNION ALLを使用してください。

> **NOTE**
>
> 各クエリ文は同じ数のカラムを返す必要があり、カラムは互換性のあるデータ型を持たなければなりません。

**例:**

テーブル`select1`と`select2`を作成します。

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

例1: 2つのテーブルのすべてのIDを重複を含めて返します。

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

例2: 2つのテーブルのすべての一意のIDを返します。以下の2つの文は同等です。

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

例3: 2つのテーブルのすべての一意のIDの中から最初の3つのIDを返します。以下の2つの文は同等です。

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

複数のクエリの結果の交差を計算し、すべての結果セットに現れる結果を返します。この句は結果セットの中で一意の行のみを返します。ALLキーワードはサポートされていません。

**構文:**

```SQL
query_1 INTERSECT [DISTINCT] query_2
```

> **NOTE**
>
> - INTERSECTはINTERSECT DISTINCTと同等です。
> - 各クエリ文は同じ数のカラムを返す必要があり、カラムは互換性のあるデータ型を持たなければなりません。

**例:**

UNIONで使用した2つのテーブルを使用します。

2つのテーブルに共通する一意の`(id, price)`の組み合わせを返します。以下の2つの文は同等です。

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

左側のクエリの結果で、右側のクエリに存在しない一意の結果を返します。EXCEPTはMINUSと同等です。

**構文:**

```SQL
query_1 {EXCEPT | MINUS} [DISTINCT] query_2
```

> **NOTE**
>
> - EXCEPTはEXCEPT DISTINCTと同等です。ALLキーワードはサポートされていません。
> - 各クエリ文は同じ数のカラムを返す必要があり、カラムは互換性のあるデータ型を持たなければなりません。

**例:**

UNIONで使用した2つのテーブルを使用します。

`select1`にあり、`select2`に見つからない一意の`(id, price)`の組み合わせを返します。

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

DISTINCTキーワードは結果セットを重複排除します。例:

```SQL
-- 1つのカラムから一意の値を返します。
select distinct tiny_column from big_table limit 2;

-- 複数のカラムから一意の値の組み合わせを返します。
select distinct tiny_column, int_column from big_table limit 2;
```

DISTINCTは集計関数（通常はcount関数）と一緒に使用でき、count (distinct)は1つ以上のカラムに含まれる異なる組み合わせがいくつあるかを計算するために使用されます。

```SQL
-- 1つのカラムから一意の値をカウントします。
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
-- 複数のカラムから一意の値の組み合わせをカウントします。
select count(distinct tiny_column, int_column) from big_table limit 2;
```

StarRocksは、複数の集計関数を同時にdistinctで使用することをサポートしています。

```SQL
-- 複数の集計関数から一意の値を個別にカウントします。
select count(distinct tiny_column, int_column), count(distinct varchar_column) from big_table;
```

### サブクエリ

サブクエリは関連性に基づいて2つのタイプに分類されます。

- 非相関サブクエリは、外部クエリとは独立して結果を取得します。
- 相関サブクエリは、外部クエリからの値を必要とします。

#### 非相関サブクエリ

非相関サブクエリは[NOT] INおよびEXISTSをサポートしています。

例:

```sql
SELECT x FROM t1 WHERE x [NOT] IN (SELECT y FROM t2);

SELECT * FROM t1 WHERE (x,y) [NOT] IN (SELECT x,y FROM t2 LIMIT 2);

SELECT x FROM t1 WHERE EXISTS (SELECT y FROM t2 WHERE y = 1);
```

v3.0以降、`SELECT... FROM... WHERE... [NOT] IN`のWHERE句で複数のフィールドを指定できます。例えば、2番目のSELECT文の`WHERE (x,y)`のように。

#### 相関サブクエリ

関連サブクエリは[NOT] INおよび[NOT] EXISTSをサポートしています。

例:

```sql
SELECT * FROM t1 WHERE x [NOT] IN (SELECT a FROM t2 WHERE t1.y = t2.b);

SELECT * FROM t1 WHERE [NOT] EXISTS (SELECT a FROM t2 WHERE t1.y = t2.b);
```

サブクエリはスカラー量子クエリもサポートしています。これは、無関係なスカラー量子クエリ、関連スカラー量子クエリ、および一般関数のパラメータとしてのスカラー量子クエリに分けられます。

例:

1. =記号を持つ無関係なスカラー量子クエリ。例えば、最高賃金の人の情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary = (SELECT MAX(salary) FROM table);
    ```

2. `>`, `<` などの述語を持つ無関係なスカラー量子クエリ。例えば、平均よりも多くの給与を受け取っている人の情報を出力します。

    ```sql
    SELECT name FROM table WHERE salary > (SELECT AVG(salary) FROM table);
    ```

3. 関連スカラー量子クエリ。例えば、各部門の最高給与情報を出力します。

    ```sql
    SELECT name FROM table a WHERE salary = (SELECT MAX(salary) FROM table b WHERE b.Department= a.Department);
    ```

4. スカラー量子クエリは通常の関数のパラメータとして使用されます。

    ```sql
    SELECT name FROM table WHERE salary = abs((SELECT MAX(salary) FROM table));
    ```

### Whereと演算子

SQL演算子は比較に使用される一連の関数であり、select文のwhere句で広く使用されます。

#### 算術演算子

算術演算子は通常、左、右、およびほとんどの場合左オペランドを含む式に現れます。

**+および-**: 単項または2項演算子として使用できます。単項演算子として使用される場合、+1、-2.5、または-col_ nameのように、値が+1または-1で乗算されることを意味します。

したがって、セル演算子+は変更されていない値を返し、セル演算子-はその値の符号ビットを変更します。

ユーザーは2つのセル演算子を重ねることができます。例えば、+5（正の値を返す）、-+2または+2（負の値を返す）ですが、ユーザーは2つの連続した-記号を使用できません。

なぜなら、以下の文では--はコメントとして解釈されるためです（ユーザーが2つの記号を使用できる場合、2つの記号の間にスペースまたは括弧が必要です。例えば、-(-2)または- -2は実際には+2になります）。

+または-が2項演算子である場合、例えば2+2、3+1.5、またはcol1+col2は、左の値が右の値に加算または減算されることを意味します。左と右の値は両方とも数値型でなければなりません。

**および/**: それぞれ乗算と除算を表します。両側のオペランドはデータ型でなければなりません。2つの数値が乗算されるとき。

必要に応じて、小さいオペランドが昇格される場合があります（例えば、SMALLINTからINTまたはBIGINTへ）、式の結果は次の大きな型に昇格されます。

例えば、TINYINTがINTと乗算されると、BIGINT型の結果が生成されます。2つの数値が乗算されるとき、両方のオペランドと式の結果は精度の損失を避けるためにDOUBLE型として解釈されます。

ユーザーが式の結果を別の型に変換したい場合、CAST関数を使用して変換する必要があります。

**%**: 剰余演算子。左オペランドを右オペランドで割った余りを返します。左オペランドと右オペランドは両方とも整数でなければなりません。

**&, | および ^**: ビット演算子は、2つのオペランドに対してビット単位のAND、ビット単位のOR、ビット単位のXOR操作の結果を返します。両方のオペランドは整数型である必要があります。

ビット演算子の2つのオペランドの型が一致しない場合、小さい型のオペランドが大きい型のオペランドに昇格され、対応するビット単位の操作が実行されます。

式には複数の算術演算子が現れることがあり、ユーザーは対応する算術式を括弧で囲むことができます。算術演算子には、算術演算子と同じ機能を表す対応する数学関数が通常ありません。

例えば、%演算子を表すMOD()関数はありません。逆に、数学関数には対応する算術演算子がありません。例えば、べき乗関数POW()には対応する**べき乗演算子がありません。

ユーザーは、数学関数セクションを通じてサポートされている算術関数を確認できます。

#### Between演算子

where句では、式を上限と下限の両方と比較することができます。式が下限以上であり、上限以下である場合、比較の結果は真です。

構文:

```sql
expression BETWEEN lower_bound AND upper_bound
```

データ型: 通常、式は数値型に評価されますが、他のデータ型もサポートしています。下限と上限が比較可能な文字であることを保証する必要がある場合、cast()関数を使用できます。

使用方法: オペランドが文字列型の場合、上限で始まる長い文字列は上限と一致しないことに注意してください。これは上限よりも大きいです。例えば、"between 'A' and 'M'"は'MJ'と一致しません。

式が正しく機能することを確認する必要がある場合、upper()、lower()、substr()、trim()などの関数を使用できます。

例:

```sql
select c1 from t1 where month between 1 and 6;
```

#### 比較演算子

比較演算子は2つの値を比較するために使用されます。`=`, `!=`, `>=`はすべてのデータ型に適用されます。

`<>` および `!=` 演算子は同等であり、2つの値が等しくないことを示します。

#### In演算子

In演算子はVALUEコレクションと比較し、コレクション内の要素のいずれかと一致する場合にTRUEを返します。

パラメータとVALUEコレクションは比較可能でなければなりません。IN演算子を使用するすべての式は、ORで接続された同等の比較として書くことができますが、INの構文はより簡潔で正確であり、StarRocksが最適化しやすくなっています。

例:

```sql
select * from small_table where tiny_column in (1,2);
```

#### Like演算子

この演算子は文字列と比較するために使用されます。''は単一の文字に一致し、'%'は複数の文字に一致します。パラメータは完全な文字列と一致する必要があります。通常、文字列の末尾に'%'を置くことがより実用的です。

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

論理演算子はBOOL値を返し、単項および複数の演算子を含みます。各演算子はBOOL値を返す式を処理するパラメータを持ちます。サポートされている演算子は次のとおりです。

AND: 2項演算子で、左と右のパラメータが両方ともTRUEと評価される場合、AND演算子はTRUEを返します。

OR: 2項演算子で、左と右のパラメータのいずれかがTRUEと評価される場合、OR演算子はTRUEを返します。両方のパラメータがFALSEの場合、OR演算子はFALSEを返します。

NOT: 単項演算子で、式の結果を反転させます。パラメータがTRUEの場合、演算子はFALSEを返します。パラメータがFALSEの場合、演算子はTRUEを返します。

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

正規表現が一致するかどうかを判断します。POSIX標準の正規表現を使用し、'^'は文字列の最初の部分に一致し、'$'は文字列の終わりに一致します。

"."は任意の単一文字に一致し、"*"はゼロまたはそれ以上のオプションに一致し、"+"は1つ以上のオプションに一致し、"?"は貪欲な表現を意味します。正規表現は文字列の一部ではなく、完全な値に一致する必要があります。

中間部分に一致させたい場合、正規表現の前の部分を'^. 'または'.'として書くことができます。'^'と'$'は通常省略されます。RLIKE演算子とREGEXP演算子は同義です。

'|'演算子はオプションの演算子です。'|'の両側の正規表現は、どちらか一方の条件を満たすだけでよいです。'|'演算子とその両側の正規表現は通常()で囲む必要があります。

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

### エイリアス

クエリ内でテーブル名、カラム名、またはカラムを含む式の名前を書くときに、エイリアスを割り当てることができます。エイリアスは通常、元の名前よりも短く、覚えやすいです。

エイリアスが必要な場合、selectリストまたはfromリスト内のテーブル、カラム、および式の名前の後にAS句を追加するだけです。ASキーワードはオプションです。ASを使用せずに、元の名前の後に直接エイリアスを指定することもできます。

エイリアスまたは他の識別子が内部の[StarRocksキーワード](../keywords.md)と同じ名前を持つ場合、名前をバッククォートで囲む必要があります。例えば、`rank`。

エイリアスは大文字と小文字を区別しますが、カラムエイリアスと式エイリアスは大文字と小文字を区別しません。

例:

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```

### PIVOT

この機能はv3.3以降でサポートされています。

PIVOT操作は、SQLの高度な機能であり、テーブル内の行を列に変換することができます。これは特にピボットテーブルを作成する際に便利です。データベースのレポートや分析で、データを要約またはカテゴリ化して表示する必要がある場合に役立ちます。

実際には、PIVOTは構文糖であり、`sum(case when ... then ... end)`のようなクエリ文の記述を簡素化できます。

#### 構文

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

#### パラメータ

PIVOT操作では、いくつかの重要なコンポーネントを指定する必要があります。

- aggregate_function(): SUM、AVG、COUNTなどの集計関数で、データを要約するために使用されます。
- alias: 集計結果のエイリアスで、結果をより理解しやすくします。
- FOR pivot_column: 行から列への変換を行うカラム名を指定します。
- IN (pivot_value): 列に変換されるpivot_columnの特定の値を指定します。

#### 例

```sql
create table t1 (c0 int, c1 int, c2 int, c3 int);
SELECT * FROM t1 PIVOT (SUM(c1) AS sum_c1, AVG(c2) AS avg_c2 FOR c3 IN (1, 2, 3, 4, 5));
-- 結果は以下のクエリと同等です。
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