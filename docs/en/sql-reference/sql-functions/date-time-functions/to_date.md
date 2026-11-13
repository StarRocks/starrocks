---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a DATETIME value into a date."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# to_date



Converts a DATETIME value into a date.

## Syntax

```plaintext
DATE TO_DATE(DATETIME datetime)
```

## Examples

```Plain Text
MySQL > select to_date("2020-02-02 00:00:00");
+--------------------------------+
| to_date('2020-02-02 00:00:00') |
+--------------------------------+
| 2020-02-02                     |
+--------------------------------+
```

## keyword

TO_DATE
