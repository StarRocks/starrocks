# arrays_overlap

## Description

Checks whether the intersection of two arrays is empty. The arrays that you specify must be of the same data type. If the intersection is empty, this function returns `0`. Otherwise, this function returns `1`.

## Syntax

```Haskell
arrays_overlap(input0, input1)
```

## Parameters

- `input0`: one of the two arrays that you want to compare.

- `input1`: the other of the two arrays that you want to compare. The value of this parameter must be of the same data type as the value of the `input0` parameter.

## Return value

Returns a value of the BOOLEAN data type.

## Examples

Example 1:

```Plain
mysql> select arrays_overlap([11, 9, 3, 2], [null, 11]);
+--------------------------------------+
| arrays_overlap([11,9,3,2], [NULL,11]) |
+--------------------------------------+
|                                    1 |
+--------------------------------------+
```

Example 2:

```Plain
mysql> select arrays_overlap([9, 3, 2], [null, 11]);
+-----------------------------------+
| arrays_overlap([9,3,2], [NULL,11]) |
+-----------------------------------+
|                                 0 |
+-----------------------------------+
```

Example 3:

```Plain
mysql> select arrays_overlap([9, 3, null, 2], [null, 11]);
+----------------------------------------+
| arrays_overlap([9,3,NULL,2], [NULL,11]) |
+----------------------------------------+
|                                      1 |
+----------------------------------------+
```

Example 4:

```Plain
mysql> select arrays_overlap([9, 3, "SQL", 2], [null, "SQL"]);
+--------------------------------------------+
| arrays_overlap([9,3,'SQL',2], [NULL,'SQL']) |
+--------------------------------------------+
|                                          1 |
+--------------------------------------------+
```
