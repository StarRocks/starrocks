---
displayed_sidebar: docs
---

# parse_json

## Description

文字列を JSON 値に変換します。

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

Example 1: STRING 値 `1` を JSON 値 `1` に変換します。

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

Example 5: 文字列が標準の JSON 値に解析できない場合、PARSE_JSON 関数は `NULL` を返します。この例では、`star` が二重引用符 (") で囲まれていないため、PARSE_JSON 関数は `NULL` を返します。

```plaintext
mysql> SELECT parse_json('{star: "rocks"}');
+-------------------------------+
| parse_json('{star: "rocks"}') |
+-------------------------------+
| NULL                          |
+-------------------------------+
```

Example 6: JSON キーに '.' が含まれる場合、例えば 'a.1' の場合、'\\' でエスケープするか、キー値全体を二重引用符で囲んで単一引用符で囲む必要があります。

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