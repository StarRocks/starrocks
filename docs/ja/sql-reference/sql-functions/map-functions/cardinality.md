---
displayed_sidebar: docs
---

# cardinality

## Description

MAP 値の要素数を返します。MAP はキーと値のペアの無順序コレクションで、例えば `{"a":1, "b":2}` のようなものです。1つのキーと値のペアが1つの要素を構成します。`{"a":1, "b":2}` は2つの要素を含んでいます。

この関数は v3.0 以降でサポートされています。[map_size()](map_size.md) のエイリアスです。

## Syntax

```Haskell
INT cardinality(any_map)
```

## Parameters

`any_map`: 要素数を取得したい MAP 値。

## Return value

INT 値を返します。

入力が NULL の場合、NULL が返されます。

MAP 値のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

## Examples

### StarRocks 内部テーブルから MAP データをクエリする

v3.1 以降、StarRocks ではテーブル作成時に MAP カラムを定義することがサポートされています。この例では、以下のデータを含むテーブル `test_map` を使用します。

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

`col_map` カラムの各行の要素数を取得します。

```Plaintext
select cardinality(col_map) from test_map order by col_int;
+----------------------+
| cardinality(col_map) |
+----------------------+
|                    2 |
|                    1 |
|                    2 |
+----------------------+
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

クラスタ内に [Hive catalog](../../../data_source/catalog/hive_catalog.md#create-a-hive-catalog) が作成された後、この catalog と cardinality() 関数を使用して、`col_map` カラムの各行の要素数を取得できます。

```Plaintext
SELECT cardinality(col_map) FROM hive_map ORDER BY col_int;
+----------------------+
| cardinality(col_map) |
+----------------------+
|                    2 |
|                    1 |
|                    2 |
+----------------------+
3 rows in set (0.05 sec)
```