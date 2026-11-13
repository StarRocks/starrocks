---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col)."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# approx_count_distinct



Returns the approximate value of aggregate function similar to the result of COUNT(DISTINCT col).

It is faster than the COUNT and DISTINCT combination and uses a fixed-size memory, so less memory is used for columns of high cardinality.

## Syntax

```plaintext
APPROX_COUNT_DISTINCT(expr)
```

## Examples

```plain text
MySQL > select approx_count_distinct(query_id) from log_statis group by datetime;
+-----------------------------------+
| approx_count_distinct(`query_id`) |
+-----------------------------------+
| 17721                             |
+-----------------------------------+
```

## keyword

APPROX_COUNT_DISTINCT
