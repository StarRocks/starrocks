---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Converts a WKT (Well Known Text) to a memory representation in the form of Line."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_LineFromText,ST_LineStringFromText



Converts a WKT (Well Known Text) to a memory representation in the form of Line.

## Syntax

```plaintext
GEOMETRY ST_LineFromText(VARCHAR wkt)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_linefromtext('LINESTRING (1 1, 2 2)'))     |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## keyword

ST_LINEFROMTEXT,ST_LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT
