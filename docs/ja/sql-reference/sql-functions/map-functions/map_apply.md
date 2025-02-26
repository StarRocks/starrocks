---
displayed_sidebar: docs
---

# map_apply

元の Map のキーと値に [Lambda expression](../Lambda_expression.md) を適用し、新しい Map を生成します。この関数は v3.0 からサポートされています。

## Syntax

```Haskell
MAP map_apply(lambda_func, any_map)
```

## Parameters

- `lambda_func`: Lambda expression。

- `any_map`: Lambda expression が適用される Map。

## Return value

Map 値を返します。結果の Map のキーと値のデータ型は、Lambda expression の結果によって決まります。

入力パラメータが NULL の場合、NULL が返されます。

元の Map のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

Lambda expression は 2 つのパラメータを持つ必要があります。最初のパラメータはキーを表します。2 番目のパラメータは値を表します。

## Examples

次の例では、[map_from_arrays()](map_from_arrays.md) を使用して Map 値 `{1:"ab",3:"cd"}` を生成します。その後、Lambda expression は各キーを 1 ずつ増加させ、各値の長さを計算します。たとえば、「ab」の長さは 2 です。

```SQL
mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays([1,3],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| {2:2,4:2}                                        |
+--------------------------------------------------+
1 row in set (0.01 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays(null,null) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
1 row in set (0.02 sec)

mysql> select map_apply((k,v)->(k+1,length(v)), col_map)
from (select map_from_arrays([1,null],["ab","cd"]) as col_map)A;
+--------------------------------------------------+
| map_apply((k, v) -> (k + 1, length(v)), col_map) |
+--------------------------------------------------+
| NULL                                             |
+--------------------------------------------------+
```