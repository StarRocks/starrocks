# replace

## Description

This function uses repl to replace a sequence of characters in str that matches pattern.
If any argument is NULL, the result is NULL.
Note: Prior to 3.0, it was implemented as [regexp_replace](../like_predicate-functions/regexp_replace.md).

## Syntax

```SQL
VARCHAR replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

## Examples

```SQL
MySQL > SELECT replace('a.b.c', '.', '+');
+----------------------------+
| replace('a.b.c', '.', '+') |
+----------------------------+
| a+b+c                      |
+----------------------------+

MySQL > SELECT replace('a b c', '', '*');
+----------------------------+
| replace('a b c', '', '*') |
+----------------------------+
| a b c                      |
+----------------------------+

MySQL > SELECT replace('We like StarRocks', 'like', '');
+------------------------------------------+
| replace('We like StarRocks', 'like', '') |
+------------------------------------------+
| We  StarRocks                            |
+------------------------------------------+

MySQL > SELECT replace('We like StarRocks', 'like', 'also like');
+---------------------------------------------------+
| replace('We like StarRocks', 'like', 'also like') |
+---------------------------------------------------+
| We also like StarRocks                            |
+---------------------------------------------------+

MySQL > SELECT replace('Do you also like StarRocks?', 'Do you also like StarRocks?', 'Yes, of course!');
+------------------------------------------------------------------------------------------+
| replace('Do you also like StarRocks?', 'Do you also like StarRocks?', 'Yes, of course!') |
+------------------------------------------------------------------------------------------+
| Yes, of course!                                                                          |
+------------------------------------------------------------------------------------------+

MySQL > SELECT replace('StarRocks is awesome', 'handsome', '404: Pattern Not Found');
+-----------------------------------------------------------------------+
| replace('StarRocks is awesome', 'handsome', '404: Pattern Not Found') |
+-----------------------------------------------------------------------+
| StarRocks is awesome                                                  |
+-----------------------------------------------------------------------+


MySQL > SELECT replace(NULL, 'a', 'z');
+-------------------------+
| replace(NULL, 'a', 'z') |
+-------------------------+
| NULL                    |
+-------------------------+

MySQL > SELECT replace('abc', NULL, 'z');
+---------------------------+
| replace('abc', NULL, 'z') |
+---------------------------+
| NULL                      |
+---------------------------+

MySQL > SELECT replace('abc', 'a', NULL);
+---------------------------+
| replace('abc', 'a', NULL) |
+---------------------------+
| NULL                      |
+---------------------------+
```

## keyword

REPLACE
