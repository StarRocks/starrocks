---
displayed_sidebar: docs
---

## strpos

Returns the position of the N-th instance of substring in string. When instance is a negative number the search will start from the end of string. Positions start with 1. If not found, 0 is returned.

## Syntax

```Haskell
INT strpos(VARCHAR str, VARCHAR substr [, INT instance])
```

## Examples

```SQL
SELECT strpos('hello world', 'world');
+-----------------------------+
| strpos('hello world', 'world') |
+-----------------------------+
|                           7 |
+-----------------------------+

SELECT strpos('Hello World', 'world');
+-----------------------------+
| strpos('Hello World', 'world') |
+-----------------------------+
|                           0 |
+-----------------------------+

SELECT strpos('hello world hello', 'hello', 2);
+--------------------------------------+
| strpos('hello world hello', 'hello', 2) |
+--------------------------------------+
|                                   13 |
+--------------------------------------+

SELECT strpos('StarRocks', 'Spark');
+----------------------------+
| strpos('StarRocks', 'Spark') |
+----------------------------+
|                          0 |
+----------------------------+

SELECT strpos(NULL, 'test');
+--------------------+
| strpos(NULL, 'test') |
+--------------------+
|               NULL |
+--------------------+
```

## Keywords

STRPOS, STRING, POSITION, SUBSTRING