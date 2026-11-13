---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the year part in a date and returns a value that ranges from 1000 to 9999."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# year



Returns the year part in a date and returns a value that ranges from 1000 to 9999.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT YEAR(DATETIME date)
```

## Examples

```Plain Text
MySQL > select year('1987-01-01');
+-----------------------------+
| year('1987-01-01 00:00:00') |
+-----------------------------+
|                        1987 |
+-----------------------------+
```

## keyword

YEAR
