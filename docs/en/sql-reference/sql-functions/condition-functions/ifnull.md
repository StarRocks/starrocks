---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "If expr1 is NULL, returns expr2."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ifnull



If `expr1` is NULL, returns expr2. If `expr1` is not NULL, returns `expr1`.

## Syntax

```plaintext
ifnull(expr1,expr2);
```

## Parameters

`expr1` and `expr2` must be compatible in data type.

## Return value

The return value has the same type as `expr1`.

## Examples

```Plain Text
mysql> select ifnull(2,4);
+--------------+
| ifnull(2, 4) |
+--------------+
|            2 |
+--------------+

mysql> select ifnull(NULL,2);
+-----------------+
| ifnull(NULL, 2) |
+-----------------+
|               2 |
+-----------------+
1 row in set (0.01 sec)
```
