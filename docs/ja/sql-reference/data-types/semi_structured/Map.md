---
displayed_sidebar: docs
---

# MAP

## 説明

MAP はキーと値のペアをセットで保存する複雑なデータ型です。例えば、`{a:1, b:2, c:3}` のように保存します。MAP のキーは一意でなければなりません。ネストされた MAP は最大 14 レベルのネストを含むことができます。

MAP データ型は v3.1 以降でサポートされています。v3.1 では、StarRocks テーブルを作成する際に MAP カラムを定義し、そのテーブルに MAP データをロードし、MAP データをクエリできます。

v2.5 以降、StarRocks はデータレイクから複雑なデータ型 MAP と STRUCT のクエリをサポートしています。StarRocks が提供する external catalog を使用して、Apache Hive™、Apache Hudi、Apache Iceberg から MAP と STRUCT データをクエリできます。ORC と Parquet ファイルからのみデータをクエリできます。external catalog を使用して外部データソースをクエリする方法の詳細については、[Overview of catalogs](../../../data_source/catalog/catalog_overview.md) と必要な catalog タイプに関連するトピックを参照してください。

## 構文

```Haskell
MAP<key_type,value_type>
```

- `key_type`: キーのデータ型。キーは StarRocks がサポートするプリミティブ型でなければなりません。例えば、数値、文字列、日付などです。HLL、JSON、ARRAY、MAP、BITMAP、STRUCT 型は使用できません。
- `value_type`: 値のデータ型。値はサポートされている任意の型で構いません。

キーと値は**ネイティブに NULL 許容**です。

## StarRocks で MAP カラムを定義する

テーブルを作成する際に MAP カラムを定義し、このカラムに MAP データをロードできます。

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

-- NOT NULL マップを定義します。
CREATE TABLE t2(
  c0 INT,
  c1 MAP<INT,DATETIME> NOT NULL
)
DUPLICATE KEY(c0);
```

MAP 型のカラムには以下の制約があります：

- テーブルのキーカラムとして使用できません。値カラムとしてのみ使用できます。
- テーブルのパーティションキー カラム（PARTITION BY に続くカラム）として使用できません。
- テーブルのバケッティング カラム（DISTRIBUTED BY に続くカラム）として使用できません。

## SQL でマップを構築する

マップは以下の 2 つの構文を使用して SQL で構築できます：

- `map{key_expr:value_expr, ...}`: マップ要素はカンマ（`,`）で区切られ、キーと値はコロン（`:`）で区切られます。例えば、`map{a:1, b:2, c:3}` です。

- `map(key_expr, value_expr ...)`: キーと値の式はペアでなければなりません。例えば、`map(a,1,b,2,c,3)` です。

StarRocks はすべての入力キーと値からキーと値のデータ型を導き出すことができます。

```SQL
select map{1:1, 2:2, 3:3} as numbers;
select map(1,1,2,2,3,3) as numbers; -- {1:1,2:2,3:3} を返します。
select map{1:"apple", 2:"orange", 3:"pear"} as fruit;
select map(1, "apple", 2, "orange", 3, "pear") as fruit; -- {1:"apple",2:"orange",3:"pear"} を返します。
select map{true:map{3.13:"abc"}, false:map{}} as nest;
select map(true, map(3.13, "abc"), false, map{}) as nest; -- {1:{3.13:"abc"},0:{}} を返します。
```

キーや値が異なる型を持つ場合、StarRocks は自動的に適切な型（スーパータイプ）を導き出します。

```SQL
select map{1:2.2, 1.2:21} as floats_floats; -- {1.0:2.2,1.2:21.0} を返します。
select map{12:"a", "100":1, NULL:NULL} as string_string; -- {"12":"a","100":"1",null:null} を返します。
```

マップを構築する際に `<>` を使用してデータ型を定義することもできます。入力キーや値は指定された型にキャストできる必要があります。

```SQL
select map<FLOAT,INT>{1:2}; -- {1:2} を返します。
select map<INT,INT>{"12": "100"}; -- {12:100} を返します。
```

キーと値は NULL 許容です。

```SQL
select map{1:NULL};
```

空のマップを構築します。

```SQL
select map{} as empty_map;
select map() as empty_map; -- {} を返します。
```

## StarRocks に MAP データをロードする

MAP データを StarRocks にロードするには、[INSERT INTO](../../../loading/InsertInto.md) と [ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の 2 つの方法があります。

StarRocks は MAP データをロードする際に各マップの重複キーを削除することに注意してください。

### INSERT INTO

```SQL
  CREATE TABLE t0(
    c0 INT,
    c1 MAP<INT,INT>
  )
  DUPLICATE KEY(c0);

  INSERT INTO t0 VALUES(1, map{1:2,3:NULL});
```

### ORC および Parquet ファイルから MAP データをロードする

StarRocks の MAP データ型は ORC または Parquet 形式のマップ構造に対応しています。追加の指定は必要ありません。[ORC/Parquet loading](../../sql-statements/loading_unloading/BROKER_LOAD.md) の指示に従って ORC または Parquet ファイルから MAP データをロードできます。

## MAP データにアクセスする

例 1: テーブル `t0` から MAP カラム `c1` をクエリします。

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

## 参考文献

- [Map functions](../../sql-functions/map-functions/map_values.md)
- [element_at](../../sql-functions/array-functions/element_at.md)