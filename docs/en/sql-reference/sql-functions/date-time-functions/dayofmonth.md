---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Obtains the day part in a date and returns a value that ranges from 1 to 31."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayofmonth



Obtains the day part in a date and returns a value that ranges from 1 to 31.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT DAYOFMONTH(DATETIME date)
```

## Examples

```Plain Text
MySQL > select dayofmonth('1987-01-31');
+-----------------------------------+
| dayofmonth('1987-01-31 00:00:00') |
+-----------------------------------+
|                                31 |
+-----------------------------------+
```

## keyword

DAYOFMONTH
