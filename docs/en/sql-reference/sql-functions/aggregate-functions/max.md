---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the maximum value of the expr expression."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# max



Returns the maximum value of the expr expression.

## Syntax

```plaintext
MAX(expr)
```

## Examples

```plain text
MySQL > select max(scan_rows)
from log_statis
group by datetime;
+------------------+
| max(`scan_rows`) |
+------------------+
|          4671587 |
+------------------+
```

## keyword

MAX
