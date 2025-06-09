---
displayed_sidebar: docs
---

# Arrow function

JSON オブジェクト内で `json_path` 式によって位置を特定できる要素をクエリし、JSON 値を返します。アロー関数 `->` は、[json_query](json_query.md) 関数よりもコンパクトで使いやすいです。

:::tip
すべての JSON 関数と演算子は、ナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## 構文

```Haskell
json_object_expr -> json_path
```

## パラメータ

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON カラムや、PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocks がサポートする JSON パス構文の詳細については、[Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## 戻り値

JSON 値を返します。

> 要素が存在しない場合、アロー関数は SQL 値の `NULL` を返します。

## 例

例 1: 指定された JSON オブジェクト内で `'$.a.b'` 式によって位置を特定できる要素をクエリします。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}') -> '$.a.b';

       -> 1
```

例 2: 入れ子になったアロー関数を使用して要素をクエリします。別のアロー関数が入れ子になったアロー関数は、入れ子になったアロー関数によって返された結果に基づいて要素をクエリします。

> この例では、ルート要素 $ は `json_path` 式から省略されています。

```plaintext
mysql> SELECT parse_json('{"a": {"b": 1}}')->'a'->'b';

       -> 1
```

例 3: 指定された JSON オブジェクト内で `'a'` 式によって位置を特定できる要素をクエリします。

> この例では、ルート要素 $ は `json_path` 式から省略されています。

```plaintext
mysql> SELECT parse_json('{"a": "b"}') -> 'a';

       -> "b"
```
