---
displayed_sidebar: docs
---

# map_size

## 説明

MAP 値の要素数を返します。

バージョン 2.5 から、StarRocks はデータレイクからの複雑なデータ型 MAP と STRUCT のクエリをサポートしています。MAP はキーと値のペアの無順序コレクションで、例えば `{"a":1, "b":2}` のようなものです。1 つのキーと値のペアが 1 つの要素を構成し、例えば `{"a":1, "b":2}` は 2 つの要素を含みます。

StarRocks が提供する external catalogs を使用して、Apache Hive™、Apache Hudi、Apache Iceberg から MAP と STRUCT データをクエリできます。ORC と Parquet ファイルからのみデータをクエリできます。external catalogs を使用して外部データソースをクエリする方法の詳細については、 [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) および必要な catalog タイプに関連するトピックを参照してください。

## 構文

```Haskell
INT map_size(any_map)
```

## パラメータ

`any_map`: 要素数を取得したい MAP 値。

## 戻り値

INT 値を返します。

入力が NULL の場合、NULL が返されます。

MAP 値のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

## 例

この例では、次のデータを含む Hive テーブル `hive_map` を使用します。

```Plain
select * from hive_map order by col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

データベースに Hive catalog が作成された後、この catalog と map_size() 関数を使用して、`col_map` 列の各行の要素数を取得できます。

```Plaintext
select map_size(col_map) from hive_map order by col_int;
+-------------------+
| map_size(col_map) |
+-------------------+
|                 2 |
|                 1 |
|                 2 |
+-------------------+
3 rows in set (0.05 sec)
```