---
displayed_sidebar: docs
---

# regexp

## Description

Checks whether a given expression matches the regular expression specified by `pattern`. If yes, 1 is returned. Otherwise, 0 is returned. NULL is returned if any of the input parameter is NULL.

regexp() supports more complex matching conditions than [like()](like.md).

## Syntax

```Haskell
BOOLEAN regexp(VARCHAR expr, VARCHAR pattern);
```

## Parameters

- `expr`: the string expression. The supported data type is VARCHAR.

- `pattern`: the pattern to match. The supported data type is VARCHAR.

## Return value

Returns a BOOLEAN value.

## Examples

```Plain Text
mysql> select regexp("abc123","abc*");
+--------------------------+
| regexp('abc123', 'abc*') |
+--------------------------+
|                        1 |
+--------------------------+
1 row in set (0.06 sec)

select regexp("abc123","xyz*");
+--------------------------+
| regexp('abc123', 'xyz*') |
+--------------------------+
|                        0 |
+--------------------------+
```

## Keywords

regexp, regular
