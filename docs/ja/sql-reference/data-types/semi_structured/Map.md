---
displayed_sidebar: docs
---

# MAP

## Description

MAP は、キーと値のペアのセットを格納する複雑なデータ型です。例えば、`{a:1, b:2, c:3}` のように使用します。マップ内のキーは一意でなければなりません。ネストされたマップは最大 14 レベルまでのネストを含むことができます。

MAP データ型は v3.1 からサポートされています。v3.1 では、StarRocks テーブルを作成する際に MAP 列を定義し、そのテーブルに MAP データをロードし、MAP データをクエリすることができます。

v2.5 以降、StarRocks はデータレイクからの複雑なデータ型 MAP および STRUCT のクエリをサポートしています。StarRocks が提供する external catalogs を使用して、Apache Hive™、Apache Hudi、および Apache Iceberg から MAP および STRUCT データをクエリできます。ORC および Parquet ファイルからのみデータをクエリできます。external catalogs を使用して外部データソースをクエリする方法の詳細については、[Overview of catalogs](../../../data_source/catalog/catalog_overview.md) および必要なカタログタイプに関連するトピックを参照してください。

## Syntax

```Haskell
MAP<key_type,value_type>
```

- `key_type`: キーのデータ型です。キーは、StarRocks がサポートするプリミティブ型（数値、文字列、日付など）でなければなりません。HLL、JSON、ARRAY、MAP、BITMAP、STRUCT 型は使用できません。
- `value_type`: 値のデータ型です。値はサポートされている任意の型であることができます。

キーと値は**ネイティブに NULL を許容**します。

## Define a MAP column in StarRocks

テーブルを作成する際に MAP 列を定義し、この列に MAP データをロードすることができます。

```SQL
-- 一次元のマップを定義します。
CREATE TABLE t0(
  c0 INT,
  c1 MAP<INT,INT>
)
DUPLICATE KEY(c0);

-- ネストされたマップを定義します。
CREATE TABLE t1(
  c0 INT,
  c1 MAP<DATE, MAP<VARCHAR(10), INT>>
)
DUPLICATE KEY(c0);

-- NOT NULL のマップを定義します。
CREATE TABLE t2(
  c0 INT,
  c1 MAP<INT,DATETIME> NOT NULL
)
DUPLICATE KEY(c0);
```

MAP 型の列には以下の制限があります：

- テーブルのキー列として使用できません。値列としてのみ使用できます。
- テーブルのパーティションキー列（PARTITION BY に続く列）として使用できません。
- テーブルのバケッティング列（DISTRIBUTED BY に続く列）として使用できません。

## Construct maps in SQL

マップは、次の 2 つの構文を使用して SQL で構築できます：

- `map{key_expr:value_expr, ...}`: マップ要素はカンマ（`,`）で区切られ、キーと値はコロン（`:`）で区切られます。例えば、`map{a:1, b:2, c:3}` のように使用します。

- `map(key_expr, value_expr ...)`: キーと値の式はペアでなければなりません。例えば、`map(a,1,b,2,c,3)` のように使用します。

StarRocks は、すべての入力キーと値からキーと値のデータ型を導出できます。

```SQL
select map{1:1, 2:2, 3:3} as numbers;
select map(1,1,2,2,3,3) as numbers; -- Return {1:1,2:2,3:3}.
select map{1:"apple", 2:"orange", 3:"pear"} as fruit;
select map(1, "apple", 2, "orange", 3, "pear") as fruit; -- Return {1:"apple",2:"orange",3:"pear"}.
select map{true:map{3.13:"abc"}, false:map{}} as nest;
select map(true, map(3.13, "abc"), false, map{}) as nest; -- Return {1:{3.13:"abc"},0:{}}.
```

キーまたは値に異なる型がある場合、StarRocks は自動的に適切な型（スーパータイプ）を導出します。

```SQL
select map{1:2.2, 1.2:21} as floats_floats; -- Return {1.0:2.2,1.2:21.0}.
select map{12:"a", "100":1, NULL:NULL} as string_string; -- Return {"12":"a","100":"1",null:null}.
```

マップを構築する際に `<>` を使用してデータ型を定義することもできます。入力キーまたは値は指定された型にキャストできる必要があります。

```SQL
select map<FLOAT,INT>{1:2}; -- Return {1:2}.
select map<INT,INT>{"12": "100"}; -- Return {12:100}.
```

キーと値は NULL を許容します。

```SQL
select map{1:NULL};
```

空のマップを構築します。

```SQL
select map{} as empty_map;
select map() as empty_map; -- Return {}.
```

## Load MAP data into StarRocks

StarRocks にマップデータをロードするには、[INSERT INTO](../../../loading/InsertInto.md) と [ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の 2 つの方法があります。

StarRocks は MAP データをロードする際に、各マップの重複したキーを削除することに注意してください。

### INSERT INTO

```SQL
  CREATE TABLE t0(
    c0 INT,
    c1 MAP<INT,INT>
  )
  DUPLICATE KEY(c0);

  INSERT INTO t0 VALUES(1, map{1:2,3:NULL});
```

### Load MAP data from ORC and Parquet files

StarRocks の MAP データ型は、ORC または Parquet 形式のマップ構造に対応しています。追加の指定は必要ありません。ORC または Parquet ファイルから MAP データをロードするには、[ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の指示に従ってください。

## Access MAP data

例 1: テーブル `t0` から MAP 列 `c1` をクエリします。

```Plain Text
mysql> select c1 from t0;
+--------------+
| c1           |
+--------------+
| {1:2,3:null} |
+--------------+
```

例 2: `[ ]` 演算子を使用してキーからマップの値を取得するか、`element_at(any_map, any_key)` 関数を使用します。

次の例では、キー `1` に対応する値をクエリします。

```Plain Text
mysql> select map{1:2,3:NULL}[1];
+-----------------------+
| map(1, 2, 3, NULL)[1] |
+-----------------------+
|                     2 |
+-----------------------+

mysql> select element_at(map{1:2,3:NULL},1);
+--------------------+
| map{1:2,3:NULL}[1] |
+--------------------+
|                  2 |
+--------------------+
```

キーがマップに存在しない場合、`NULL` が返されます。

次の例では、存在しないキー 2 に対応する値をクエリします。

```Plain Text
mysql> select map{1:2,3:NULL}[2];
+-----------------------+
| map(1, 2, 3, NULL)[2] |
+-----------------------+
|                  NULL |
+-----------------------+
```

例 3: 多次元マップを**再帰的に**クエリします。

次の例では、まずキー `1` に対応する値 `map{2:1}` をクエリし、その後 `map{2:1}` 内のキー `2` に対応する値を再帰的にクエリします。

```Plain Text
mysql> select map{1:map{2:1},3:NULL}[1][2];

+----------------------------------+
| map(1, map(2, 1), 3, NULL)[1][2] |
+----------------------------------+
|                                1 |
+----------------------------------+
```

## References

- [Map functions](../../sql-functions/map-functions/map_values.md)
- [element_at](../../sql-functions/array-functions/element_at.md)