---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns a string of the specified number of spaces."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# space



Returns a string of the specified number of spaces.

## Syntax

```plaintext
space(x);
```

## Parameters

`x`: the number of spaces to return. The supported data type is INT.

## Return value

Returns a value of the VARCHAR type.

## Examples

```Plain Text
mysql> select space(6);
+----------+
| space(6) |
+----------+
|          |
+----------+
1 row in set (0.00 sec)
```

## Keywords

SPACE
