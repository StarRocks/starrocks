---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Returns the Y coordinate of a GEOGRAPHY or GEOMETRY point."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_Y



If point is of a valid Point type, return the corresponding Y-coordinate value.

## Syntax

```plaintext
DOUBLE ST_Y(POINT point)
```

## Examples

```Plain Text
MySQL > SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
| st_y(st_point(24.7, 56.7)) |
+----------------------------+
|                       56.7 |
+----------------------------+
```

## keyword

ST_Y,ST,Y
