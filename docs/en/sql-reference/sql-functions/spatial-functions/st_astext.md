---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a GEOMETRY or GEOGRAPHY value to WKT (Well-Known Text)."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_AsText,ST_AsWKT



Converts a geometric figure to WKT (Well Known Text) format.

## Syntax

```plaintext
VARCHAR ST_AsText(GEOMETRY geo)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## keyword

ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
