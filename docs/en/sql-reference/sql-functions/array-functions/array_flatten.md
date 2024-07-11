---
displayed_sidebar: "English"
---

# array_flatten

## Description

Flatten one layer of nested arrays.

## Syntax

```Haskell
array_flatten(param)
```

## Parameters

`param`: a nested array that needs to be flattened. Only nested arrays are supported, and it can be a multi-level nested array.

## Examples

Example 1: Flatten a 2-level nested array.

```plaintext
mysql> SELECT array_flatten([[1, 2], [1, 4]]) as res;
+-----------+
| res       |
+-----------+
| [1,2,1,4] |
+-----------+
```

Example 2: Flatten a 3-level nested array.

```plaintext
mysql> SELECT array_flatten([[[1],[2]], [[3],[4]]]) as res;
+-------------------+
| res               |
+-------------------+
| [[1],[2],[3],[4]] |
+-------------------+
```
