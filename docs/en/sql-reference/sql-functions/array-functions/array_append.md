---
displayed_sidebar: docs
---

# array_append

## Description

Adds a new element to the end of the array. Returns an array.

## Syntax

```Haskell
array_append(any_array, any_element)
```

## Examples

```plain text
mysql> select array_append([1, 2], 3);
+------------------------+
| array_append([1,2], 3) |
+------------------------+
| [1,2,3]                |
+------------------------+
1 row in set (0.00 sec)

```

You can add NULL to the array.

```plain text
mysql> select array_append([1, 2], NULL);
+---------------------------+
| array_append([1,2], NULL) |
+---------------------------+
| [1,2,NULL]                |
+---------------------------+
1 row in set (0.01 sec)

```

## keyword

ARRAY_APPEND,ARRAY
