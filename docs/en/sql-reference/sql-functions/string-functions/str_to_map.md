# str_to_map

## Description

Splits a given string into key-value pairs using two delimiters and returns a map of the split pairs.

This function is supported from v3.1 onwards.

## Syntax

```Haskell
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## Parameters

- `content`: required, the string expression to split.
- `delimiter`: optional, the delimiter used to split `content` into key-value pairs, defaults to `,`.
- `map_delimiter`: optional, the delimiter used to separate each key-value pair, defaults to `:`.

## Return value

Returns a MAP of STRING elements. Any null input results in NULL.

## Examples

```SQL
mysql> SELECT str_to_map('a:1|b:2|c:3', '|', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a:1;b:2;c:3', ';', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a:1,b:2,c:3', ',', ':') as map;
+---------------------------+
| map                       |
+---------------------------+
| {"a":"1","b":"2","c":"3"} |
+---------------------------+

mysql> SELECT str_to_map('a') as map;
+------------+
| map        |
+------------+
| {"a":null} |
+------------+

mysql> SELECT str_to_map('a:1,b:2,c:3',null, ':') as map;
+------+
| map  |
+------+
| NULL |
+------+

mysql> SELECT str_to_map('a:1,b:2,c:null') as map;
+------------------------------+
| map                          |
+------------------------------+
| {"a":"1","b":"2","c":"null"} |
+------------------------------+
```

## keywords

STR_TO_MAP
