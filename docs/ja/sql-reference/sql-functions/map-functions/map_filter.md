---
displayed_sidebar: docs
---

# map_filter

マップ内のキーと値のペアを、Boolean 配列または各キーと値のペアに対して [Lambda 式](../Lambda_expression.md) を適用することでフィルタリングします。`true` と評価されるペアが返されます。

この関数は v3.1 以降でサポートされています。

## 構文

```Haskell
MAP map_filter(any_map, array<boolean>)
MAP map_filter(lambda_func, any_map)
```

- `map_filter(any_map, array<boolean>)`

  `any_map` のキーと値のペアを `array<boolean>` に対して一つずつ評価し、`true` と評価されるキーと値のペアを返します。

- `map_filter(lambda_func, any_map)`

  `lambda_func` を `any_map` のキーと値のペアに一つずつ適用し、結果が `true` となるキーと値のペアを返します。

## パラメータ

- `any_map`: マップの値。

- `array<boolean>`: マップの値を評価するために使用される Boolean 配列。

- `lambda_func`: マップの値を評価するために使用される Lambda 式。

## 戻り値

`any_map` と同じデータ型のマップを返します。

`any_map` が NULL の場合、NULL が返されます。`array<boolean>` が null の場合、空のマップが返されます。

マップの値のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

Lambda 式は 2 つのパラメータを持つ必要があります。最初のパラメータはキーを表します。2 番目のパラメータは値を表します。

## 例

### `array<boolean>` を使用

次の例では、[map_from_arrays()](map_from_arrays.md) を使用してマップ値 `{1:"ab",3:"cdd",2:null,null:"abc"}` を生成します。その後、各キーと値のペアを `array<boolean>` に対して評価し、結果が `true` となるペアが返されます。

```SQL
mysql> select map_filter(col_map, array<boolean>[0,0,0,1,1]) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------------------+
| map_filter(col_map, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+----------------------------------------------------+
| {null:"abc"}                                       |
+----------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(null, array<boolean>[0,0,0,1,1]);
+-------------------------------------------------+
| map_filter(NULL, ARRAY<BOOLEAN>[0, 0, 0, 1, 1]) |
+-------------------------------------------------+
| NULL                                            |
+-------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_filter(col_map, null) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+---------------------------+
| map_filter(col_map, NULL) |
+---------------------------+
| {}                        |
+---------------------------+
1 row in set (0.01 sec)
```

### Lambda 式を使用

次の例では、map_from_arrays() を使用してマップ値 `{1:"ab",3:"cdd",2:null,null:"abc"}` を生成します。その後、各キーと値のペアを Lambda 式に対して評価し、値が null でないキーと値のペアが返されます。

```SQL

mysql> select map_filter((k,v) -> v is not null,col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------------+
| map_filter((k,v) -> v is not null, col_map)    |
+------------------------------------------------+
| {1:"ab",3:"cdd",null:'abc'}                        |
+------------------------------------------------+
1 row in set (0.02 sec)
```