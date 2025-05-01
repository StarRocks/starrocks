---
displayed_sidebar: docs
---

# map_values

## 説明

指定されたマップ内のすべての値を配列として返します。

バージョン 2.5 から、StarRocks はデータレイクからの複雑なデータ型 MAP および STRUCT のクエリをサポートしています。MAP はキーと値のペアの順序のないコレクションで、例えば `{"a":1, "b":2}` のようなものです。

StarRocks が提供する external catalogs を使用して、Apache Hive™、Apache Hudi、および Apache Iceberg から MAP および STRUCT データをクエリできます。ORC および Parquet ファイルからのみデータをクエリできます。external catalogs を使用して外部データソースをクエリする方法の詳細については、 [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) および必要な catalog タイプに関連するトピックを参照してください。

## 構文

```Haskell
map_values(any_map)
```

## パラメータ

`any_map`: 値を取得したい MAP 値。

## 戻り値

戻り値は `array<valueType>` の形式です。配列内の要素の型はマップ内の値の型と一致します。

入力が NULL の場合、NULL が返されます。MAP 値のキーまたは値が NULL の場合、NULL は通常の値として処理され、結果に含まれます。

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

データベースに Hive catalog が作成された後、この catalog と map_values() 関数を使用して、`col_map` 列の各行からすべての値を取得できます。

```SQL
select map_values(col_map) from hive_map order by col_int;
+---------------------+
| map_values(col_map) |
+---------------------+
| [1,2]               |
| [3]                 |
| [4,5]               |
+---------------------+
3 rows in set (0.04 sec)
```