---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the hour for a given date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# hour



Returns the hour for a given date. The return value ranges from 0 to 23.

The `date` parameter must be of the DATE or DATETIME type.

## Syntax

```plaintext
INT HOUR(DATETIME|DATE date)
```

## Examples

```Plain Text
MySQL > select hour('2018-12-31 23:59:59');
+-----------------------------+
| hour('2018-12-31 23:59:59') |
+-----------------------------+
|                          23 |
+-----------------------------+
```

## keyword

HOUR
