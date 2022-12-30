# like

## Description

Checks whether a given expression fuzzy matches the specified pattern. If yes, 1 is returned. Otherwise, 0 is returned. NULL is returned if any of the input parameter is NULL.

LIKE is usually used together with characters such as the percent sign (%) and underscore (_). `%` matches 0, 1, or more characters. `_` matches any single character.

## Syntax

```Haskell
BOOLEAN like(VARCHAR expr, VARCHAR pattern);
```

## Parameters

- `expr`: the string expression. The supported data type is VARCHAR.

- `pattern`: the pattern to match. The supported data type is VARCHAR.

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
