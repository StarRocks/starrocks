---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the name of the month for a given date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# monthname



Returns the name of the month for a given date.  

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
VARCHAR MONTHNAME(date)
```

## Examples

```Plain Text
MySQL > select monthname('2008-02-03 00:00:00');
+----------------------------------+
| monthname('2008-02-03 00:00:00') |
+----------------------------------+
| February                         |
+----------------------------------+
```

## keyword

MONTHNAME, monthname
