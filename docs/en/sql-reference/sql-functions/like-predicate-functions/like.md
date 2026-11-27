---
displayed_sidebar: docs
description: "Checks whether a given expression fuzzy matches the specified pattern."
products: [oss, enterprise, cloud, anywhere]
---

# like



Checks whether a given expression fuzzy matches the specified pattern. If yes, 1 is returned. Otherwise, 0 is returned. NULL is returned if any of the input parameter is NULL.

LIKE is usually used together with characters such as the percent sign (%) and underscore (_). `%` matches 0, 1, or more characters. `_` matches any single character.

## Syntax

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## Parameters

- `expr`: the string expression. The supported data type is VARCHAR.

- `pattern`: the pattern to match. The supported data type is VARCHAR.

## Escape character

To match a literal `%` or `_`, put the escape character in front of it. The default escape character is the backslash, which is written as `\\` in a string literal.

When LIKE is used as an operator, an ESCAPE clause can set a different escape character for the pattern:

```SQL
expr [NOT] LIKE pattern ESCAPE 'escape_char'
```

`escape_char` is a single character, or an empty string if the pattern has no escape character. With an ESCAPE clause, `pattern` must be a constant expression.

```SQL
-- Returns the rows in which c1 ends with "50%".
SELECT * FROM t1 WHERE c1 LIKE '%50^%' ESCAPE '^';
```

## Return value

Returns a BOOLEAN value.

## Examples

```Plain Text
mysql> select like("star","star");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+

mysql> select like("starrocks","star%");
+----------------------+
| like('star', 'star') |
+----------------------+
|                    1 |
+----------------------+

mysql> select like("starrocks","star_");
+----------------------------+
| like('starrocks', 'star_') |
+----------------------------+
|                          0 |
+----------------------------+
```
