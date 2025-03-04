---
displayed_sidebar: docs
---

# map_values

指定されたマップ内のすべての値を含む配列を返します。

この関数は v2.5 からサポートされています。

## 構文

```Haskell
map_values(any_map)
```

## パラメータ

`any_map`: 値を取得したい MAP 値。

## 戻り値

戻り値は `array<valueType>` の形式です。配列内の要素の型は、マップ内の値の型と一致します。

入力が NULL の場合、NULL が返されます。MAP 値のキーまたは値が NULL の場合、NULL は通常の値として処理され、結果に含まれます。

## 例

### StarRocks 内部テーブルから MAP データをクエリする

v3.1 以降、StarRocks はテーブル作成時に MAP カラムを定義することをサポートしています。この例では、以下のデータを含むテーブル `test_map` を使用します。

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

`col_map` カラムの各行からすべての値を取得します。

```SQL
select map_values(col_map) from test_map order by col_int;
+---------------------+
| map_values(col_map) |
+---------------------+
| [1,2]               |
| [3]                 |
| [4,5]               |
+---------------------+
3 rows in set (0.04 sec)
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

クラスター内に [Hive catalog](../../../data_source/catalog/hive_catalog.md#create-a-hive-catalog) が作成された後、この catalog と map_values() 関数を使用して、`col_map` カラムの各行からすべての値を取得できます。

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