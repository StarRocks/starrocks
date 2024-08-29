---
displayed_sidebar: docs
---

# parse_json

## Description

Converts a string to a JSON value.

## Syntax

```Haskell
parse_json(string_expr)
```

## Parameters

`string_expr`: the expression that represents the string. Only the STRING, VARCHAR, and CHAR data types are supported.

## Return value

Returns a JSON value.

> Note: If the string cannot be parsed into a standard JSON value, the PARSE_JSON function returns `NULL` (see Example 5). For information about the JSON specifications, see [RFC 7159](https://tools.ietf.org/html/rfc7159?spm=a2c63.p38356.0.0.14d26b9fcp7fcf#page-4).

## Examples

Example 1: Convert a STRING value of `1` to a JSON value of `1`.

```plaintext
mysql> SELECT parse_json('1');
+-----------------+
| parse_json('1') |
+-----------------+
| "1"             |
+-----------------+
```

Example 2: Convert an array of the STRING data type to a JSON array.

```plaintext
mysql> SELECT parse_json('[1,2,3]');
+-----------------------+
| parse_json('[1,2,3]') |
+-----------------------+
| [1, 2, 3]             |
+-----------------------+ 
```

Example 3: Convert an object of the STRING data type to a JSON object.

```plaintext
mysql> SELECT parse_json('{"star": "rocks"}');
+---------------------------------+
| parse_json('{"star": "rocks"}') |
+---------------------------------+
| {"star": "rocks"}               |
+---------------------------------+
```

Example 4: Construct a JSON value of `NULL`.

```plaintext
mysql> SELECT parse_json('null');
+--------------------+
| parse_json('null') |
+--------------------+
| "null"             |
+--------------------+
```

Example 5: If the string cannot be parsed into a standard JSON value, the PARSE_JSON function returns `NULL`. In this example, `star` is not enclosed in double quotation marks ("). Therefore, the PARSE_JSON function returns `NULL`.

```plaintext
mysql> SELECT parse_json('{star: "rocks"}');
+-------------------------------+
| parse_json('{star: "rocks"}') |
+-------------------------------+
| NULL                          |
+-------------------------------+
```

Example 6: If a JSON key contains a '.', for example, 'a.1', it must be escaped with '\\' or you need to enclose the entire key value along with double quotes in single quotes.


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
