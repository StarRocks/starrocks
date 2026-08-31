---
displayed_sidebar: docs
description: "Converts a geometric figure to WKT (Well Known Text) format."
---

# ST_AsText,ST_AsWKT



Converts a native `GEOMETRY` value or a legacy geography value to Well-Known Text (WKT).

## Syntax

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsText(VARCHAR legacy_geo)
VARCHAR ST_AsWKT(VARCHAR legacy_geo)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('MULTIPOINT (1 2, 3 4)'));
+--------------------------------------------------------------+
| st_astext(st_geomfromtext('MULTIPOINT (1 2, 3 4)'))          |
+--------------------------------------------------------------+
| MULTIPOINT ((1 2), (3 4))                                    |
+--------------------------------------------------------------+
```

## keyword

ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
