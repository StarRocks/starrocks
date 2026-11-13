---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the difference between two DATETIME expressions."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# timediff



Returns the difference between two DATETIME expressions.

The return value must be of the TIME type.

## Syntax

```plaintext
TIME TIMEDIFF(DATETIME expr1, DATETIME expr2)`
```

## Examples

```Plain Text
MySQL > SELECT TIMEDIFF(now(),utc_timestamp());
+----------------------------------+
| timediff(now(), utc_timestamp()) |
+----------------------------------+
| 08:00:00                         |
+----------------------------------+

MySQL > SELECT TIMEDIFF('2019-07-11 16:59:30','2019-07-11 16:59:21');
+--------------------------------------------------------+
| timediff('2019-07-11 16:59:30', '2019-07-11 16:59:21') |
+--------------------------------------------------------+
| 00:00:09                                               |
+--------------------------------------------------------+

MySQL > SELECT TIMEDIFF('2019-01-01 00:00:00', NULL);
+---------------------------------------+
| timediff('2019-01-01 00:00:00', NULL) |
+---------------------------------------+
| NULL                                  |
+---------------------------------------+
```

## keyword

TIMEDIFF
