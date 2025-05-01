---
displayed_sidebar: docs
---

# transform_values

## Description

マップ内の値を [Lambda expression](../Lambda_expression.md) を使用して変換し、マップ内の各エントリに対して新しい値を生成します。

この関数は v3.1 以降でサポートされています。

## Syntax

```Haskell
MAP transform_values(lambda_func, any_map)
```

`lambda_func` は `any_map` の後に配置することもできます:

```Haskell
MAP transform_values(any_map, lambda_func)
```

## Parameters

- `any_map`: マップ。

- `lambda_func`: `any_map` に適用したい Lambda expression。

## Return value

Lambda expression の結果によって値のデータ型が決定され、キーのデータ型は `any_map` のキーと同じであるマップ値を返します。

入力パラメータが NULL の場合、NULL が返されます。

元のマップ内のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

Lambda expression は2つのパラメータを持つ必要があります。最初のパラメータはキーを表します。2番目のパラメータは値を表します。

## Examples

次の例では [map_from_arrays](map_from_arrays.md) を使用して、マップ値 `{1:"ab",3:"cdd",2:null,null:"abc"}` を生成します。その後、Lambda expression がマップの各値に適用されます。最初の例では、各キーと値のペアの値を 1 に変更します。2番目の例では、各キーと値のペアの値を null に変更します。

```SQL
mysql> select transform_values((k,v)->1, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+----------------------------------------+
| transform_values((k, v) -> 1, col_map) |
+----------------------------------------+
| {1:1,3:1,2:1,null:1}                   |
+----------------------------------------+
1 row in set (0.02 sec)

mysql> select transform_values((k,v)->null, col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+--------------------------------------------+
| transform_values((k, v) -> NULL, col_map)  |
+--------------------------------------------+
| {1:null,3:null,2:null,null:null} |
+--------------------------------------------+
1 row in set (0.01 sec)
```