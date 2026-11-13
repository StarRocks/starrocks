---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the day of the year for a given date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayofyear



Returns the day of the year for a given date.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT DAYOFYEAR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select dayofyear('2007-02-03 00:00:00');
+----------------------------------+
| dayofyear('2007-02-03 00:00:00') |
+----------------------------------+
|                               34 |
+----------------------------------+
```

## keyword

DAYOFYEAR
