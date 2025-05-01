---
displayed_sidebar: docs
---

# map_keys

## 説明

指定されたマップ内のすべてのキーを配列として返します。

バージョン 2.5 から、StarRocks はデータレイクから複雑なデータ型 MAP および STRUCT のクエリをサポートしています。MAP はキーと値のペアの無順序コレクションで、例えば `{"a":1, "b":2}` です。

StarRocks が提供する external catalogs を使用して、Apache Hive™、Apache Hudi、および Apache Iceberg から MAP および STRUCT データをクエリできます。クエリできるのは ORC および Parquet ファイルからのデータのみです。external catalogs を使用して外部データソースをクエリする方法の詳細については、 [Overview of catalogs](../../../data_source/catalog/catalog_overview.md) および必要な catalog タイプに関連するトピックを参照してください。

## 構文

```Haskell
map_keys(any_map)
```

## パラメータ

`any_map`: キーを取得したい MAP 値。

## 戻り値

戻り値は `array<keyType>` の形式です。配列内の要素タイプは、マップ内のキータイプと一致します。

入力が NULL の場合、NULL が返されます。MAP 値内のキーまたは値が NULL の場合、NULL は通常の値として処理され、結果に含まれます。

## 例

この例では、次のデータを含む Hive テーブル `hive_map` を使用します:

```SQL
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

データベースに Hive catalog が作成された後、この catalog と map_keys() 関数を使用して、`col_map` 列の各行からすべてのキーを取得できます。

```Plain
select map_keys(col_map) from hive_map order by col_int;
+-------------------+
| map_keys(col_map) |
+-------------------+
| ["a","b"]         |
| ["c"]             |
| ["d","e"]         |
+-------------------+
3 rows in set (0.05 sec)
```