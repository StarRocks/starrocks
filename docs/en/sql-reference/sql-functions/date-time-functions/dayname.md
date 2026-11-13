---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the day corresponding to a date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# dayname



Returns the day corresponding to a date.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
VARCHAR DAYNAME(date)
```

## Examples

```Plain Text
MySQL > select dayname('2007-02-03 00:00:00');
+--------------------------------+
| dayname('2007-02-03 00:00:00') |
+--------------------------------+
| Saturday                       |
+--------------------------------+
```

## keyword

DAYNAME
