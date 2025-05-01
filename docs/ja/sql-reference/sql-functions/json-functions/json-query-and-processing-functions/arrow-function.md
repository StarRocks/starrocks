---
displayed_sidebar: docs
---

# Arrow function

## 説明

JSONオブジェクト内で`json_path`式によって位置を特定できる要素をクエリし、JSON値を返します。アロー関数`->`は、[json_query](json_query.md)関数よりもコンパクトで使いやすいです。

## 構文

```Haskell
json_object_expr -> json_path
```

## パラメータ

- `json_object_expr`: JSONオブジェクトを表す式。このオブジェクトは、JSONカラムやPARSE_JSONのようなJSONコンストラクタ関数によって生成されたJSONオブジェクトである可能性があります。

- `json_path`: JSONオブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocksがサポートするJSONパス構文の詳細については、 [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## 戻り値

JSON値を返します。

> 要素が存在しない場合、アロー関数はSQL値の`NULL`を返します。

## 例

例1: 指定されたJSONオブジェクト内で`'$.a.b'`式によって位置を特定できる要素をクエリします。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

例2: ネストされたアロー関数を使用して要素をクエリします。別のアロー関数がネストされたアロー関数は、ネストされたアロー関数によって返される結果に基づいて要素をクエリします。

> この例では、`json_path`式からルート要素$が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

例3: 指定されたJSONオブジェクト内で`'a'`式によって位置を特定できる要素をクエリします。

> この例では、`json_path`式からルート要素$が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```