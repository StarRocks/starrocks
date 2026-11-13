---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "Constructs native GEOMETRY from WKT with an explicit CRS, or uses the legacy one-argument format."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_GeometryFromText,ST_GeomFromText



Converts a WKT (Well Known Text) to the corresponding memory geometry.

## Syntax

```plaintext
GEOMETRY ST_GeometryFromText(VARCHAR wkt)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## keyword

ST_GEOMETRYFROMTEXT,ST_GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT
