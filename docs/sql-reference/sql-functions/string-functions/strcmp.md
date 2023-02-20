# strcmp

## Description

This function compares two strings. Returns 0 if lhs and rhs compare equal. Return -1 if lhs appears before rhs in lexicographical order. Returns 1 if lhs appears after rhs in lexicographical order. When the argument is NULL, the result is NULL.

## Syntax

```Haskell
INT strcmp(VARCHAR lhs, VARCHAR rhs)
```

## Examples

```Plain Text
mysql> select strcmp("test1", "test1");
+--------------------------+
| strcmp('test1', 'test1') |
+--------------------------+
|                        0 |
+--------------------------+

mysql> select strcmp("test1", "test2");
+--------------------------+
| strcmp('test1', 'test2') |
+--------------------------+
|                       -1 |
+--------------------------+

mysql> select strcmp("test2", "test1");
+--------------------------+
| strcmp('test2', 'test1') |
+--------------------------+
|                        1 |
+--------------------------+

mysql> select strcmp("test1", NULL);
+-----------------------+
| strcmp('test1', NULL) |
+-----------------------+
|                  NULL |
+-----------------------+
```

## keyword

STRCMP
