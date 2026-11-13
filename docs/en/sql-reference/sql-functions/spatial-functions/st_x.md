---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the X coordinate of a GEOGRAPHY or GEOMETRY point."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_X



If point is of a valid Point type, return the corresponding X-coordinate value.

## Syntax

```plaintext
DOUBLE ST_X(POINT point)
```

## Examples

```Plain Text
MySQL > SELECT ST_X(ST_Point(24.7, 56.7));
+----------------------------+
| st_x(st_point(24.7, 56.7)) |
+----------------------------+
|                       24.7 |
+----------------------------+
```

## keyword

ST_X,ST,X
