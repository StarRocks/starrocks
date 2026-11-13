---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the week number for a given date within a year."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# weekofyear



Returns the week number for a given date within a year.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT WEEKOFYEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select weekofyear('2008-02-20 00:00:00');
+-----------------------------------+
| weekofyear('2008-02-20 00:00:00') |
+-----------------------------------+
|                                 8 |
+-----------------------------------+
```

## keyword

WEEKOFYEAR
