---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the second part for a given date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# second



Returns the second part for a given date. The return value ranges from 0 to 59.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT SECOND(DATETIME date)
```

## Examples

```Plain Text
MySQL > select second('2018-12-31 23:59:59');
+-----------------------------+
|second('2018-12-31 23:59:59')|
+-----------------------------+
|                          59 |
+-----------------------------+
```

## keyword

SECOND
