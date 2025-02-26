---
displayed_sidebar: docs
---

# STRUCT

## 説明

STRUCT は複雑なデータ型を表現するために広く使用されています。異なるデータ型を持つ要素（フィールドとも呼ばれます）のコレクションを表します。例えば、`<a INT, b STRING>` のように。

STRUCT 内のフィールド名は一意でなければなりません。フィールドは、プリミティブなデータ型（数値、文字列、日付など）や複雑なデータ型（ARRAY や MAP など）であることができます。

STRUCT 内のフィールドは、別の STRUCT、ARRAY、または MAP にすることもでき、これによりネストされたデータ構造を作成できます。例えば、`STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>` のように。

STRUCT データ型は v3.1 以降でサポートされています。v3.1 では、StarRocks テーブルを作成する際に STRUCT カラムを定義し、そのテーブルに STRUCT データをロードし、MAP データをクエリできます。

v2.5 以降、StarRocks はデータレイクからの複雑なデータ型 MAP および STRUCT のクエリをサポートしています。StarRocks が提供する external catalog を使用して、Apache Hive™、Apache Hudi、Apache Iceberg から MAP および STRUCT データをクエリできます。データは ORC および Parquet ファイルからのみクエリできます。external catalog を使用して外部データソースをクエリする方法については、[Overview of catalogs](../../../data_source/catalog/catalog_overview.md) および必要な catalog タイプに関連するトピックを参照してください。

## 構文

```Haskell
STRUCT<name, type>
```

- `name`: フィールド名で、CREATE TABLE ステートメントで定義されたカラム名と同じです。
- `type`: フィールドの型です。サポートされている任意の型を指定できます。

## StarRocks で STRUCT カラムを定義する

テーブルを作成する際に STRUCT カラムを定義し、このカラムに STRUCT データをロードできます。

```SQL
-- 一次元の struct を定義します。
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

-- 複雑な struct を定義します。
CREATE TABLE t1(
  c0 INT,
  c1 STRUCT<a INT, b STRUCT<c INT, d INT>, c MAP<INT, INT>, d ARRAY<INT>>
)
DUPLICATE KEY(c0);

-- NOT NULL の struct を定義します。
CREATE TABLE t2(
  c0 INT,
  c1 STRUCT<a INT, b INT> NOT NULL
)
DUPLICATE KEY(c0);
```

STRUCT 型のカラムには以下の制約があります：

- テーブルのキー カラムとして使用できません。値カラムとしてのみ使用できます。
- テーブルのパーティションキー カラム（PARTITION BY の後）として使用できません。
- テーブルのバケッティング カラム（DISTRIBUTED BY の後）として使用できません。
- [集計テーブル](../../../table_design/table_types/aggregate_table.md) で値カラムとして使用する場合、replace() 関数のみをサポートします。

## SQL で struct を構築する

STRUCT は、次の関数を使用して SQL で構築できます：[row, struct](../../sql-functions/struct-functions/row.md)、および [named_struct](../../sql-functions/struct-functions/named_struct.md)。struct() は row() のエイリアスです。

- `row` と `struct` は名前のない struct をサポートします。フィールド名を指定する必要はありません。StarRocks は自動的に `col1`, `col2`... のようなカラム名を生成します。
- `named_struct` は名前付き struct をサポートします。名前と値の式はペアでなければなりません。

StarRocks は入力値に基づいて自動的に struct の型を決定します。

```SQL
select row(1, 2, 3, 4) as numbers; -- {"col1":1,"col2":2,"col3":3,"col4":4} を返します。
select row(1, 2, null, 4) as numbers; -- {"col1":1,"col2":2,"col3":null,"col4":4} を返します。
select row(null) as nulls; -- {"col1":null} を返します。
select struct(1, 2, 3, 4) as numbers; -- {"col1":1,"col2":2,"col3":3,"col4":4} を返します。
select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4) as numbers; -- {"a":1,"b":2,"c":3,"d":4} を返します。
```

## STRUCT データのロード

STRUCT データを StarRocks にロードするには、[INSERT INTO](../../../loading/InsertInto.md) と [ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の2つの方法があります。

StarRocks はデータ型を対応する STRUCT 型に自動的にキャストすることに注意してください。名前付き struct カラムにデータをロードする際は、テーブル作成時に指定した順序で struct 内のフィールド名が並んでいることを確認してください。

### INSERT INTO

```SQL
CREATE TABLE t0(
  c0 INT,
  c1 STRUCT<a INT, b INT>
)
DUPLICATE KEY(c0);

INSERT INTO t0 VALUES(1, row(1, 1));

SELECT * FROM t0;
+------+---------------+
| c0   | c1            |
+------+---------------+
|    1 | {"a":1,"b":1} |
+------+---------------+
```

### ORC/Parquet ファイルから STRUCT データをロードする

StarRocks の STRUCT データ型は、ORC または Parquet 形式のネストされたカラム構造に対応しています。追加の指定は必要ありません。[ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の指示に従って、ORC または Parquet ファイルから STRUCT データをロードできます。

## STRUCT フィールドへのアクセス

struct のサブフィールドをクエリするには、ドット (`.`) 演算子を使用してフィールド名で値をクエリするか、`[]` を使用してインデックスで値を呼び出すことができます。

```Plain Text
mysql> select named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a;
+------------------------------------------------+
| named_struct('a', 1, 'b', 2, 'c', 3, 'd', 4).a |
+------------------------------------------------+
| 1                                              |
+------------------------------------------------+

mysql> select row(1, 2, 3, 4).col1;
+-----------------------+
| row(1, 2, 3, 4).col1  |
+-----------------------+
| 1                     |
+-----------------------+

mysql> select row(2, 4, 6, 8)[2];
+--------------------+
| row(2, 4, 6, 8)[2] |
+--------------------+
|                  4 |
+--------------------+

mysql> select row(map{'a':1}, 2, 3, 4)[1];
+-----------------------------+
| row(map{'a':1}, 2, 3, 4)[1] |
+-----------------------------+
| {"a":1}                     |
+-----------------------------+
```