---
displayed_sidebar: docs
---

# ARRAY

ARRAY はデータベースの拡張型として、PostgreSQL、ClickHouse、Snowflake などのさまざまなデータベースシステムでサポートされています。ARRAY は、A/B テスト、ユーザータグ分析、ユーザープロファイリングなどのシナリオで広く使用されています。StarRocks は多次元配列のネスト、配列のスライス、比較、フィルタリングをサポートしています。

## ARRAY カラムの定義

テーブルを作成する際に ARRAY カラムを定義できます。

~~~SQL
-- 一次元配列を定義します。
ARRAY<type>

-- ネストされた配列を定義します。
ARRAY<ARRAY<type>>

-- 配列カラムを NOT NULL として定義します。
ARRAY<type> NOT NULL
~~~

`type` は配列内の要素のデータ型を指定します。StarRocks は次の要素型をサポートしています: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, FLOAT, DOUBLE, VARCHAR, CHAR, DATETIME, DATE, JSON, ARRAY (v3.1 以降), MAP (v3.1 以降), STRUCT (v3.1 以降)。

配列内の要素はデフォルトで NULL 可能です。例えば、`[null, 1 ,2]` のように指定できます。配列内の要素を NOT NULL として指定することはできませんが、テーブルを作成する際に ARRAY カラムを NOT NULL として指定することは可能です。以下のコードスニペットの 3 番目の例を参照してください。

例:

~~~SQL
-- c1 を要素型が INT の一次元配列として定義します。
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);

-- c1 を要素型が VARCHAR のネストされた配列として定義します。
create table t1(
  c0 INT,
  c1 ARRAY<ARRAY<VARCHAR(10)>>
)
duplicate key(c0)
distributed by hash(c0);

-- c1 を NOT NULL の配列カラムとして定義します。
create table t2(
  c0 INT,
  c1 ARRAY<INT> NOT NULL
)
duplicate key(c0)
distributed by hash(c0);
~~~

## 制限事項

StarRocks テーブルで ARRAY カラムを作成する際には、次の制限が適用されます:

- v2.1 より前のバージョンでは、ARRAY カラムは重複キーテーブルでのみ作成できます。v2.1 以降では、他のタイプのテーブル（Primary Key、Unique Key、Aggregate）でも ARRAY カラムを作成できます。ただし、集計テーブルでは、そのカラムでデータを集計するために使用される関数が replace() または replace_if_not_null() の場合にのみ ARRAY カラムを作成できます。詳細は [集計テーブル](../../../table_design/table_types/aggregate_table.md) を参照してください。
- ARRAY カラムはキー カラムとして使用できません。
- ARRAY カラムはパーティションキー（PARTITION BY に含まれる）またはバケッティングキー（DISTRIBUTED BY に含まれる）として使用できません。
- DECIMAL V3 は ARRAY でサポートされていません。
- 配列は最大 14 レベルのネストが可能です。

## SQL での配列の構築

配列は、角括弧 `[]` を使用して SQL で構築でき、各配列要素はカンマ（`,`）で区切られます。

~~~Plain Text
mysql> select [1, 2, 3] as numbers;

+---------+
| numbers |
+---------+
| [1,2,3] |
+---------+

mysql> select ["apple", "orange", "pear"] as fruit;

+---------------------------+
| fruit                     |
+---------------------------+
| ["apple","orange","pear"] |
+---------------------------+

mysql> select [true, false] as booleans;

+----------+
| booleans |
+----------+
| [1,0]    |
+----------+
~~~

StarRocks は、配列が複数の型の要素で構成されている場合、データ型を自動的に推論します。

~~~Plain Text
mysql> select [1, 1.2] as floats;
+---------+
| floats  |
+---------+
| [1.0,1.2] |
+---------+

mysql> select [12, "100"];

+--------------+
| [12,'100']   |
+--------------+
| ["12","100"] |
+--------------+
~~~

尖括弧（`<>`）を使用して宣言された配列型を示すことができます。

~~~Plain Text
mysql> select ARRAY<float>[1, 2];

+-----------------------+
| ARRAY<float>[1.0,2.0] |
+-----------------------+
| [1,2]                 |
+-----------------------+

mysql> select ARRAY<INT>["12", "100"];

+------------------------+
| ARRAY<int(11)>[12,100] |
+------------------------+
| [12,100]               |
+------------------------+
~~~

要素に NULL を含めることができます。

~~~Plain Text
mysql> select [1, NULL];

+----------+
| [1,NULL] |
+----------+
| [1,null] |
+----------+
~~~

空の配列の場合、尖括弧を使用して宣言された型を示すことができます。または、StarRocks がコンテキストに基づいて型を推論するために `[]` を直接記述することもできます。StarRocks が型を推論できない場合、エラーが報告されます。

~~~Plain Text
mysql> select [];

+------+
| []   |
+------+
| []   |
+------+

mysql> select ARRAY<VARCHAR(10)>[];

+----------------------------------+
| ARRAY<unknown type: NULL_TYPE>[] |
+----------------------------------+
| []                               |
+----------------------------------+

mysql> select array_append([], 10);

+----------------------+
| array_append([], 10) |
+----------------------+
| [10]                 |
+----------------------+
~~~

## 配列データのロード

StarRocks は、配列データのロードを 3 つの方法でサポートしています:

- INSERT INTO は、小規模データのテスト用のロードに適しています。
- Broker Load は、大規模データの ORC または Parquet ファイルのロードに適しています。
- Stream Load と Routine Load は、大規模データの CSV ファイルのロードに適しています。

### INSERT INTO を使用して配列をロードする

INSERT INTO を使用して、小規模データをカラムごとにロードしたり、データをロードする前に ETL を実行したりできます。

  ~~~SQL
create table t0(
  c0 INT,
  c1 ARRAY<INT>
)
duplicate key(c0)
distributed by hash(c0);

INSERT INTO t0 VALUES(1, [1,2,3]);
~~~

### Broker Load を使用して ORC または Parquet ファイルから配列をロードする

  StarRocks の配列型は、ORC および Parquet ファイルのリスト構造に対応しており、StarRocks で異なるデータ型を指定する必要がありません。データロードの詳細については、[Broker load](../../sql-statements/loading_unloading/BROKER_LOAD.md) を参照してください。

### Stream Load または Routine Load を使用して CSV 形式の配列をロードする

  CSV ファイル内の配列はデフォルトでカンマで区切られています。[Stream Load または Routine Load](../../../loading/Loading_intro.md) を使用して、CSV テキストファイルまたは Kafka 内の CSV データをロードできます。

## 配列データのクエリ

配列内の要素には、`[]` と添字を使用してアクセスできます。添字は `1` から始まります。

~~~Plain Text
mysql> select [1,2,3][1];

+------------+
| [1,2,3][1] |
+------------+
|          1 |
+------------+
1 row in set (0.00 sec)
~~~

添字が 0 または負の数の場合、**エラーは報告されず、NULL が返されます**。

~~~Plain Text
mysql> select [1,2,3][0];

+------------+
| [1,2,3][0] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

添字が配列の長さ（配列内の要素数）を超える場合、**NULL が返されます**。

~~~Plain Text
mysql> select [1,2,3][4];

+------------+
| [1,2,3][4] |
+------------+
|       NULL |
+------------+
1 row in set (0.01 sec)
~~~

多次元配列の場合、要素は**再帰的に**アクセスできます。

~~~Plain Text
mysql(ARRAY)> select [[1,2],[3,4]][2];

+------------------+
| [[1,2],[3,4]][2] |
+------------------+
| [3,4]            |
+------------------+
1 row in set (0.00 sec)

mysql> select [[1,2],[3,4]][2][1];

+---------------------+
| [[1,2],[3,4]][2][1] |
+---------------------+
|                   3 |
+---------------------+
1 row in set (0.01 sec)
~~~