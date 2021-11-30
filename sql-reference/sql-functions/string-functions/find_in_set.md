# find_in_set

## description

### Syntax

```Haskell
INT find_in_set(VARCHAR str, VARCHAR strlist)
```

This function returns the position of the first str in strlist (start counting with 1). Strlist is a sting separated by commas. If it does not find any str, it returns 0. When the argument is NULL, the result is NULL.

## example

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
