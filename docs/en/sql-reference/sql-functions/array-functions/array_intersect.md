---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns an array of the elements in the intersection of one or more arrays."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# array_intersect



Returns an array of the elements in the intersection of one or more arrays.

## Syntax

```plaintext
array_intersect(input0, input1, ...)
```

## Parameters

`input`: one or more arrays whose intersection you want to obtain. Specify arrays in the `(input0, input1, ...)` format and make sure that the arrays that you specify are of the same data type.

## Return value

Returns an array of the same data type as the arrays that you specify.

## Examples

Example 1:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", "query", "SQL"], ["SQL"])
AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| ["SQL"]      |
+--------------+
```

Example 2:

```Plain
mysql> SELECT array_intersect(["SQL", "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| []           |
+--------------+
```

Example 3:

```Plain
mysql> SELECT array_intersect(["SQL", null, "storage"], ["mysql", null], [null]) AS no_intersect ;
+--------------+
| no_intersect |
+--------------+
| [null]       |
+--------------+
```
