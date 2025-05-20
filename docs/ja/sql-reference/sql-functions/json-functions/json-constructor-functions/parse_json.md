---
displayed_sidebar: docs
---

# parse_json

文字列を JSON 値に変換します。

:::tip
すべての JSON 関数と演算子はナビゲーションと [overview page](../overview-of-json-functions-and-operators.md) に一覧されています。

クエリを [生成列](../../../sql-statements/generated_columns.md) で高速化しましょう。
:::

## Syntax

```Haskell
parse_json(string_expr)
```

## Parameters

`string_expr`: 文字列を表す式。STRING、VARCHAR、CHAR データ型のみがサポートされています。

## Return value

JSON 値を返します。

> Note: 文字列が標準の JSON 値に解析できない場合、PARSE_JSON 関数は `NULL` を返します（例 5 を参照）。JSON の仕様については、[RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4) を参照してください。

## Examples

Example 1: STRING 値の `1` を JSON 値の `1` に変換します。

```plaintext
mysql> SELECT parse_json('1');
+-----------------+
| parse_json('1') |
+-----------------+
| "1"             |
+-----------------+
```

Example 2: STRING データ型の配列を JSON 配列に変換します。

```plaintext
mysql> SELECT parse_json('[1,2,3]');
+-----------------------+
| parse_json('[1,2,3]') |
+-----------------------+
| [1, 2, 3]             |
+-----------------------+ 
```

Example 3: STRING データ型のオブジェクトを JSON オブジェクトに変換します。

```plaintext
mysql> SELECT parse_json('{"star": "rocks"}');
+---------------------------------+
| parse_json('{"star": "rocks"}') |
+---------------------------------+
| {"star": "rocks"}               |
+---------------------------------+
```

Example 4: `NULL` の JSON 値を構築します。

```plaintext
mysql> SELECT parse_json('null');
+--------------------+
| parse_json('null') |
+--------------------+
| "null"             |
+--------------------+
```

Example 5: 文字列が標準の JSON 値に解析できない場合、PARSE_JSON 関数は `NULL` を返します。この例では、`star` がダブルクォーテーションマーク (") で囲まれていないため、PARSE_JSON 関数は `NULL` を返します。

```plaintext
mysql> SELECT parse_json('{star: "rocks"}');
+-------------------------------+
| parse_json('{star: "rocks"}') |
+-------------------------------+
| NULL                          |
+-------------------------------+
```

Example 6: JSON キーに '.' が含まれる場合、例えば 'a.1'、それは '\\' でエスケープするか、キー値全体をダブルクォーテーションで囲んでシングルクォーテーションで囲む必要があります。

```plaintext
mysql> select parse_json('{"b":4, "a.1": "1"}')->"a\\.1";
+--------------------------------------------+
| parse_json('{"b":4, "a.1": "1"}')->'a\\.1' |
+--------------------------------------------+
| "1"                                        |
+--------------------------------------------+
mysql> select parse_json('{"b":4, "a.1": "1"}')->'"a.1"';
+--------------------------------------------+
| parse_json('{"b":4, "a.1": "1"}')->'"a.1"' |
+--------------------------------------------+
| "1"                                        |
+--------------------------------------------+
```

## Keywords

parse_json, parse json