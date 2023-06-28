# str_to_map

## Description

Splits a given string into key-value pairs using two delimiters and returns a map of the split pairs. The first delimiter splits the string into key-value pairs and the second delimiter splits each key-value pair.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## Parameters

- `content`: required, the STRING expression to split.
- `delimiter`: optional, the delimiter used to split `content` into key-value pairs, defaults to `,`.
- `map_delimiter`: optional, the delimiter used to separate each key-value pair, defaults to `:`.

## Return value

Returns a MAP of STRING elements. Any null input results in NULL.

## Examples

```SQL
mysql> SELECT str_to_map('a:1,b:2,c:3', ',', ':');
+--------------------------------------------+
| str_to_map(split('a:1,b:2,c:3', ','), ':') |
+--------------------------------------------+
| {"a":"1","b":"2","c":"3"}                  |
+--------------------------------------------+

mysql> SELECT str_to_map('a:1,b:2,c:3');
+--------------------------------------------+
| str_to_map(split('a:1,b:2,c:3', ','), ':') |
+--------------------------------------------+
| {"a":"1","b":"2","c":"3"}                  |
+--------------------------------------------+

mysql> SELECT str_to_map('a');
+----------------------------------+
| str_to_map(split('a', ','), ':') |
+----------------------------------+
| {"a":null}                       |
+----------------------------------+

mysql> SELECT str_to_map('a:1,b:2,c:3',null, ':');
+---------------------------------------------+
| str_to_map(split('a:1,b:2,c:3', NULL), ':') |
+---------------------------------------------+
| NULL                                        |
+---------------------------------------------+
```

## keywords

STR_TO_MAP
