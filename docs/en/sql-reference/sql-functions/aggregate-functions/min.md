---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the minimum value of the expr expression."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# min



Returns the minimum value of the expr expression.

## Syntax

```plaintext
MIN(expr)
```

## Examples

```plain text
MySQL > select min(scan_rows)
from log_statis
group by datetime;
+------------------+
| min(`scan_rows`) |
+------------------+
|                0 |
+------------------+
```

## keyword

MIN
