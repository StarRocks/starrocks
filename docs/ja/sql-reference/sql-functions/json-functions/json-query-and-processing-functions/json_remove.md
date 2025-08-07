---
displayed_sidebar: docs
---

# json_remove

指定されたJSONパスからデータを削除し、修正されたJSONドキュメントを返します。

:::tip
すべてのJSON関数と演算子は、ナビゲーションと[概要ページ](../overview-of-json-functions-and-operators.md)に記載されています
:::

## 構文

```Haskell
json_remove(json_object_expr, json_path[, json_path] ...)
```

## パラメータ

- `json_object_expr`: JSONオブジェクトを表す式。オブジェクトはJSONカラム、またはPARSE_JSONなどのJSONコンストラクタ関数によって生成されたJSONオブジェクトです。

- `json_path`: JSONオブジェクト内の削除すべき要素へのパスを表す1つ以上の式。各パラメータの値は文字列です。StarRocksでサポートされているJSONパス構文については、[JSON関数と演算子の概要](../overview-of-json-functions-and-operators.md)を参照してください。

## 戻り値

指定されたパスが削除されたJSONドキュメントを返します。

> - パスがJSONドキュメントに存在しない場合、無視されます。
> - 無効なパスが提供された場合、無視されます。
> - すべてのパスが無効または存在しない場合、元のJSONドキュメントが変更されずに返されます。

## 例

例1: JSONオブジェクトから単一のキーを削除します。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.a');

       -> {"b": [10, 20, 30]}
```

例2: JSONオブジェクトから複数のキーを削除します。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30], "c": "test"}', '$.a', '$.c');

       -> {"b": [10, 20, 30]}
```

例3: JSONオブジェクトから配列要素を削除します。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": [10, 20, 30]}', '$.b[1]');

       -> {"a": 1, "b": [10, 30]}
```

例4: ネストされたオブジェクトのプロパティを削除します。

```plaintext
mysql> SELECT json_remove('{"a": {"x": 1, "y": 2}, "b": 3}', '$.a.x');

       -> {"a": {"y": 2}, "b": 3}
```

例5: 存在しないパスの削除を試行します（無視されます）。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2}', '$.c', '$.d');

       -> {"a": 1, "b": 2}
```

例6: 存在しないパスを含む複数のパスを削除します。

```plaintext
mysql> SELECT json_remove('{"a": 1, "b": 2, "c": 3}', '$.a', '$.nonexistent', '$.c');

       -> {"b": 2}
```

## 使用上の注意

- `json_remove`関数はMySQL互換の動作に従います。
- 無効なJSONパスはエラーを発生させることなく静かに無視されます。
- この関数は単一の操作で複数のパスを削除することをサポートしており、複数の個別操作よりも効率的です。
- 現在、この関数は単純なオブジェクトキーの削除（例：`$.key`）をサポートしています。複雑なネストされたパスや配列要素の削除のサポートは、現在の実装では制限される場合があります。 