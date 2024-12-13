---
displayed_sidebar: docs
---

# find_in_set

<<<<<<< HEAD
## Description
=======

>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))

This function returns the position of the first str in strlist (start counting with 1). Strlist is a sting separated by commas. If it does not find any str, it returns 0. When the argument is NULL, the result is NULL.

## Syntax

```Haskell
INT find_in_set(VARCHAR str, VARCHAR strlist)
```

## Examples

```Plain Text
MySQL > select find_in_set("b", "a,b,c");
+---------------------------+
| find_in_set('b', 'a,b,c') |
+---------------------------+
|                         2 |
+---------------------------+
```

## keyword

FIND_IN_SET,FIND,IN,SET
