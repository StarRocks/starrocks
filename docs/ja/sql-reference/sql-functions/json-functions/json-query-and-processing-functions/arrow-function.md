---
displayed_sidebar: docs
---

# Arrow function

`json_path` 式で特定できる要素を JSON オブジェクトからクエリし、JSON 値を返します。矢印関数 `->` は、[json_query](json_query.md) 関数よりもコンパクトで使いやすいです。

## Syntax

```Haskell
json_object_expr -> json_path
```

## Parameters

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトである可能性があります。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocks がサポートする JSON パス構文の詳細については、[Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## Return value

JSON 値を返します。

> 要素が存在しない場合、矢印関数は SQL 値 `NULL` を返します。

## Examples

Example 1: 指定された JSON オブジェクト内で `'$.a.b'` 式で特定できる要素をクエリします。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

Example 2: ネストされた矢印関数を使用して要素をクエリします。ネストされた矢印関数の結果に基づいて要素をクエリする矢印関数です。

> この例では、`json_path` 式からルート要素 $ が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

Example 3: 指定された JSON オブジェクト内で `'a'` 式で特定できる要素をクエリします。

> この例では、`json_path` 式からルート要素 $ が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```

:::tip
すべての JSON 関数と演算子は、ナビゲーションおよび [overview page](../overview-of-json-functions-and-operators.md) に一覧表示されています。
:::