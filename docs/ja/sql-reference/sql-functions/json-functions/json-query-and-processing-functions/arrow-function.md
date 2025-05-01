---
displayed_sidebar: docs
---

# Arrow function

## 説明

JSON オブジェクト内で `json_path` 式によって特定できる要素をクエリし、JSON 値を返します。アロー関数 `->` は、[json_query](json_query.md) 関数よりもコンパクトで使いやすいです。

## 構文

```Haskell
json_object_expr -> json_path
```

## パラメーター

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトである可能性があります。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメーターの値は文字列です。StarRocks がサポートする JSON パス構文の詳細については、 [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## 戻り値

JSON 値を返します。

> 要素が存在しない場合、アロー関数は SQL 値 `NULL` を返します。

## 例

例 1: 指定された JSON オブジェクト内で `'$.a.b'` 式によって特定できる要素をクエリします。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

例 2: ネストされたアロー関数を使用して要素をクエリします。別のアロー関数がネストされたアロー関数は、ネストされたアロー関数によって返される結果に基づいて要素をクエリします。

> この例では、`json_path` 式からルート要素 $ が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

例 3: 指定された JSON オブジェクト内で `'a'` 式によって特定できる要素をクエリします。

> この例では、`json_path` 式からルート要素 $ が省略されています。

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```