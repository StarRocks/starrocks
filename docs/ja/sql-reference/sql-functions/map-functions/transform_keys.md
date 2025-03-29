---
displayed_sidebar: docs
---

# transform_keys

マップ内のキーを [Lambda expression](../Lambda_expression.md) を使用して変換し、マップ内の各エントリに対して新しいキーを生成します。

この関数は v3.1 以降でサポートされています。

## 構文

```Haskell
MAP transform_keys(lambda_func, any_map)
```

`lambda_func` は `any_map` の後に置くこともできます:

```Haskell
MAP transform_keys(any_map, lambda_func)
```

## パラメータ

- `any_map`: マップ。

- `lambda_func`: `any_map` に適用したい Lambda expression。

## 戻り値

キーのデータ型は Lambda expression の結果によって決定され、値のデータ型は `any_map` 内の値と同じであるマップ値を返します。

入力パラメータが NULL の場合、NULL が返されます。

元のマップ内のキーまたは値が NULL の場合、NULL は通常の値として処理されます。

Lambda expression は 2 つのパラメータを持つ必要があります。最初のパラメータはキーを表します。2 番目のパラメータは値を表します。

## 例

次の例では、[map_from_arrays](map_from_arrays.md) を使用してマップ値 `{1:"ab",3:"cdd",2:null,null:"abc"}` を生成します。その後、Lambda expression を各キーに適用してキーを 1 増やします。

```SQL
mysql> select transform_keys((k,v)->(k+1), col_map) from (select map_from_arrays([1,3,null,2,null],['ab','cdd',null,null,'abc']) as col_map)A;
+------------------------------------------+
| transform_keys((k, v) -> k + 1, col_map) |
+------------------------------------------+
| {2:"ab",4:"cdd",3:null,null:"abc"}       |
+------------------------------------------+
```