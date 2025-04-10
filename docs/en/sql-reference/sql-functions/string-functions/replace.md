---
displayed_sidebar: docs
---

# replace

## Description

Replaces all occurrences of characters in a string with another string. This function performs a case-sensitive match when searching for `pattern`.

This function is supported from v3.0.

Note: Prior to 3.0, this function was implemented as [regexp_replace](../like-predicate-functions/regexp_replace.md).

## Syntax

```SQL
VARCHAR replace(VARCHAR str, VARCHAR pattern, VARCHAR repl)
```

## Parameters

- `str`: the original string.

- `pattern`: the characters to replace. Note that this is not a regular expression.

- `repl`: the string used to replace characters in `pattern`.

## Return value

Returns a string with the specified characters replaced.

If any argument is NULL, the result is NULL.

If no matching characters are found, the original string is returned.

## Examples

```plain
-- Replace '.' in 'a.b.c' with '+'.

MySQL > SELECT replace('a.b.c', '.', '+');
+----------------------------+
| replace('a.b.c', '.', '+') |
+----------------------------+
| a+b+c                      |
+----------------------------+

-- No matching characters are found and the original string is returned.

MySQL > SELECT replace('a b c', '', '*');
+----------------------------+
| replace('a b c', '', '*') |
+----------------------------+
| a b c                      |
+----------------------------+

-- Replace 'like' with an empty string.

MySQL > SELECT replace('We like StarRocks', 'like', '');
+------------------------------------------+
| replace('We like StarRocks', 'like', '') |
+------------------------------------------+
| We  StarRocks                            |
+------------------------------------------+

-- No matching characters are found and the original string is returned.

MySQL > SELECT replace('He is awesome', 'handsome', 'smart');
+-----------------------------------------------+
| replace('He is awesome', 'handsome', 'smart') |
+-----------------------------------------------+
| He is awesome                                 |
+-----------------------------------------------+
```

## keywords

REPLACE, replace
