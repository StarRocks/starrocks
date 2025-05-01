---
displayed_sidebar: docs
---

# json_query

## 説明

`json_path` 式で特定できる要素の値を JSON オブジェクトからクエリし、JSON 値を返します。

## 構文

```Haskell
json_query(json_object_expr, json_path)
```

## パラメータ

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON カラムや、PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトであることができます。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocks がサポートする JSON パス構文については、 [Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## 戻り値

JSON 値を返します。

> 要素が存在しない場合、json_query 関数は SQL 値の `NULL` を返します。

## 例

例 1: 指定された JSON オブジェクト内で `'$.a.b'` 式で特定できる要素の値をクエリします。この例では、json_query 関数は JSON 値の `1` を返します。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

例 2: 指定された JSON オブジェクト内で `'$.a.c'` 式で特定できる要素の値をクエリします。この例では、要素が存在しないため、json_query 関数は SQL 値の `NULL` を返します。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> NULL
```

例 3: 指定された JSON オブジェクト内で `'$.a[2]'` 式で特定できる要素の値をクエリします。この例では、配列 a という名前の JSON オブジェクトにインデックス 2 の要素が含まれており、その要素の値は 3 です。したがって、JSON_QUERY 関数は JSON 値の `3` を返します。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 3
```

例 4: 指定された JSON オブジェクト内で `'$.a[3]'` 式で特定できる要素をクエリします。この例では、配列 a という名前の JSON オブジェクトにインデックス 3 の要素が含まれていません。したがって、json_query 関数は SQL 値の `NULL` を返します。

```plaintext
mysql> SELECT json_query(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> NULL
```