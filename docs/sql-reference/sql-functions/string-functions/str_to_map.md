# str_to_map

## Description

This function splits a given string into key-value pairs of string according to two delimiters, and returns the split pairs in MAP.

## Syntax

```SQL
MAP<VARCHAR, VARCHAR> str_to_map(VARCHAR content[, VARCHAR delimiter[, VARCHAR map_delimiter]])
```

## Parameters
`delimiter` splits `content` into sub strings in an ARRAY, like [split(content, delimiter)](split.md), default by `,`;

`map_delimiter` separates each sub-string into a key-value pair, default by `:`;

## Results
Any null input results into NULL.

## Examples

```SQL
mysql> select str_to_map(c,',',b), c, b, split(c,',') from t;
+------------------------------+---------------+------+----------------------+
| str_to_map(split(c, ','), b) | c             | b    | split(c, ',')        |
+------------------------------+---------------+------+----------------------+
| NULL                         | a:b,null:null | NULL | ["a:b","null:null"]  |
| {"":null}                    |               | ,    | [""]                 |
| {"a":"","c:d过’":null}       | a:b,c:d过’    | :b   | ["a:b","c:d过’"]     |
| {"a":":b"}                   | a:b           |      | ["a:b"]              |
| {"a":"c:b:d","":null}        | a:c:b:d,,,    | :    | ["a:c:b:d","","",""] |
| NULL                         | NULL          | :    | NULL                 |
+------------------------------+---------------+------+----------------------+
6 rows in set (0.02 sec)
```

## keyword

STR_TO_MAP
