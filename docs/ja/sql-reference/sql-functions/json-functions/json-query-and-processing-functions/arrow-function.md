---
displayed_sidebar: docs
---

# Arrow function

`json_path` 式で特定できる JSON オブジェクト内の要素をクエリし、JSON 値を返します。アロー関数 `->` は、[json_query](json_query.md) 関数よりもコンパクトで使いやすいです。

:::tip
すべての JSON 関数と演算子は、ナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
json_object_expr -> json_path
```

## Parameters

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON 列、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocks がサポートする JSON パス構文については、[Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## Return value

JSON 値を返します。

> 要素が存在しない場合、アロー関数は SQL 値 `NULL` を返します。

## Examples

Example 1: 指定された JSON オブジェクト内で `'$.a.b'` 式で特定できる要素をクエリします。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

Example 2: ネストされたアロー関数を使用して要素をクエリします。別のアロー関数がネストされたアロー関数は、ネストされたアロー関数によって返される結果に基づいて要素をクエリします。

> この例では、ルート要素 $ は `json_path` 式から省略されています。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

Example 3: 指定された JSON オブジェクト内で `'a'` 式で特定できる要素をクエリします。

> この例では、ルート要素 $ は `json_path` 式から省略されています。

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```