# split

## Description

This function splits a given string according to the separators, and returns the split parts in ARRAY.

## Syntax

```SQL
ARRAY<VARCHAR> split(VARCHAR content, VARCHAR delimiter)
```

## Examples

```SQL
mysql> select split("a,b,c",",");
+---------------------+
| split('a,b,c', ',') |
+---------------------+
| ["a","b","c"]       |
+---------------------+

mysql> select split("a,b,c",",b,");
+-----------------------+
| split('a,b,c', ',b,') |
+-----------------------+
| ["a","c"]             |
+-----------------------+

mysql> select split("abc","");
+------------------+
| split('abc', '') |
+------------------+
| ["a","b","c"]    |
+------------------+
```

## keyword

SPLIT
