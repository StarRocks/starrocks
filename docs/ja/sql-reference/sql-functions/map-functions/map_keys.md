---
displayed_sidebar: docs
---

# map_keys

指定されたマップ内のすべてのキーを配列として返します。

この関数は v2.5 からサポートされています。

## Syntax

```Haskell
map_keys(any_map)
```

## Parameters

`any_map`: キーを取得したい MAP 値。

## Return value

返り値は `array<keyType>` の形式です。配列内の要素の型はマップ内のキーの型と一致します。

入力が NULL の場合、NULL が返されます。MAP 値内のキーまたは値が NULL の場合、NULL は通常の値として処理され、結果に含まれます。

## Examples

### StarRocks 内部テーブルから MAP データをクエリする

v3.1 以降、StarRocks はテーブル作成時に MAP カラムの定義をサポートしています。この例では、以下のデータを含むテーブル `test_map` を使用します。

```Plain
CREATE TABLE test_map(
    col_int INT,
    col_map MAP<VARCHAR(50),INT>
  )
DUPLICATE KEY(col_int);

INSERT INTO test_map VALUES
(1,map{"a":1,"b":2}),
(2,map{"c":3}),
(3,map{"d":4,"e":5});

SELECT * FROM test_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
3 rows in set (0.05 sec)
```

`col_map` カラムの各行からすべてのキーを取得します。

```Plaintext
select map_keys(col_map) from test_map order by col_int;
+-------------------+
| map_keys(col_map) |
+-------------------+
| ["a","b"]         |
| ["c"]             |
| ["d","e"]         |
+-------------------+
3 rows in set (0.05 sec)
```

### データレイクから MAP データをクエリする

この例では、以下のデータを含む Hive テーブル `hive_map` を使用します。

```Plaintext
SELECT * FROM hive_map ORDER BY col_int;
+---------+---------------+
| col_int | col_map       |
+---------+---------------+
|       1 | {"a":1,"b":2} |
|       2 | {"c":3}       |
|       3 | {"d":4,"e":5} |
+---------+---------------+
```

クラスター内に [Hive catalog](../../../data_source/catalog/hive_catalog.md#create-a-hive-catalog) が作成された後、この catalog と map_keys() 関数を使用して、`col_map` カラムの各行からすべてのキーを取得できます。

```Plaintext
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