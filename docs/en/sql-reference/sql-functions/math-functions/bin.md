---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Transforms the input arg into a binary."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# bin



Transforms the input `arg` into a binary.

## Syntax

```Shell
bin(arg)
```

## Parameter

`arg`: the input you want to transform into a binary. It supports the BIGINT data type.

## Return value

Returns a value of the VARCHAR data type.

## Examples

```Plain
mysql> select bin(3);
+--------+
| bin(3) |
+--------+
| 11     |
+--------+
1 row in set (0.02 sec)
```
