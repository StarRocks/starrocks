---
displayed_sidebar: docs
---

# array_contains

## Description

Checks whether the array contains a certain element. If yes, it returns 1; otherwise, it returns 0.

## Syntax

```Haskell
array_contains(any_array, any_element)
```

## Examples

```plain text
mysql> select array_contains(["apple","orange","pear"], "orange");
+-----------------------------------------------------+
| array_contains(['apple','orange','pear'], 'orange') |
+-----------------------------------------------------+
|                                                   1 |
+-----------------------------------------------------+
1 row in set (0.01 sec)
```

You can also check whether the array contains NULL.

```plain text
mysql> select array_contains([1, NULL], NULL);
+--------------------------------+
| array_contains([1,NULL], NULL) |
+--------------------------------+
|                              1 |
+--------------------------------+
1 row in set (0.00 sec)
```

You can check whether the multi-dimensional array contains a certain subarray. At this time, you need to ensure that the subarray elements match exactly, including the element arrangement order.

```plain text
mysql> select array_contains([[1,2,3], [4,5,6]], [4,5,6]);
+--------------------------------------------+
| array_contains([[1,2,3],[4,5,6]], [4,5,6]) |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
1 row in set (0.00 sec)

mysql> select array_contains([[1,2,3], [4,5,6]], [4,6,5]);
+--------------------------------------------+
| array_contains([[1,2,3],[4,5,6]], [4,6,5]) |
+--------------------------------------------+
|                                          0 |
+--------------------------------------------+
1 row in set (0.00 sec)
```

## keyword

ARRAY_CONTAINS,ARRAY
