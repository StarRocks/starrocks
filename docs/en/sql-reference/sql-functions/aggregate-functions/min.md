---
displayed_sidebar: docs
products: [oss, enterprise, cloud, anywhere]
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
