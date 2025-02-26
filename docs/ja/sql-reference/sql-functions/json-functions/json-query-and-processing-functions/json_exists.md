---
displayed_sidebar: docs
---

# json_exists

JSON オブジェクトが `json_path` 式によって特定できる要素を含んでいるかどうかを確認します。要素が存在する場合、JSON_EXISTS 関数は `1` を返します。そうでない場合、JSON_EXISTS 関数は `0` を返します。

## Syntax

```Haskell
json_exists(json_object_expr, json_path)
```

## Parameters

- `json_object_expr`: JSON オブジェクトを表す式。このオブジェクトは JSON カラム、または PARSE_JSON などの JSON コンストラクタ関数によって生成された JSON オブジェクトである可能性があります。

- `json_path`: JSON オブジェクト内の要素へのパスを表す式。このパラメータの値は文字列です。StarRocks がサポートする JSON パス構文の詳細については、[Overview of JSON functions and operators](../overview-of-json-functions-and-operators.md) を参照してください。

## Return value

BOOLEAN 値を返します。

## Examples

例 1: 指定された JSON オブジェクトが `'$.a.b'` 式によって特定できる要素を含んでいるかどうかを確認します。この例では、要素は JSON オブジェクトに存在します。したがって、json_exists 関数は `1` を返します。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.b') ;

       -> 1
```

例 2: 指定された JSON オブジェクトが `'$.a.c'` 式によって特定できる要素を含んでいるかどうかを確認します。この例では、要素は JSON オブジェクトに存在しません。したがって、json_exists 関数は `0` を返します。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": {"b": 1}}'), '$.a.c') ;

       -> 0
```

例 3: 指定された JSON オブジェクトが `'$.a[2]'` 式によって特定できる要素を含んでいるかどうかを確認します。この例では、配列 a という名前の JSON オブジェクトがインデックス 2 に要素を含んでいます。したがって、json_exists 関数は `1` を返します。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[2]') ;

       -> 1
```

例 4: 指定された JSON オブジェクトが `'$.a[3]'` 式によって特定できる要素を含んでいるかどうかを確認します。この例では、配列 a という名前の JSON オブジェクトがインデックス 3 に要素を含んでいません。したがって、json_exists 関数は `0` を返します。

```plaintext
mysql> SELECT json_exists(PARSE_JSON('{"a": [1,2,3]}'), '$.a[3]') ;

       -> 0
```