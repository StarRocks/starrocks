---
displayed_sidebar: docs
---

# strpos

Returns the position of the N-th substring in a string. If N is a negative number, the search will start from the end of the string. Positions start with 1. If not found, 0 is returned.

## Syntax

```Haskell
INT strpos(VARCHAR str, VARCHAR substr [, INT instance])
```

## Parameters

- `str`: The string in which to find the substring.
- `substr`: The substring to find.
- `instance`: The N-th substring instance to find. If this item is set to a negative value, the search will start from the end of the string. Default Value: `1`.

## Return value

Returns an integer. `0` is returned if the substring is not found.

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