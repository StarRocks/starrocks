---
displayed_sidebar: docs
---

# ST_AsText,ST_AsWKT

## Description

Converts a geometric figure to WKT (Well Known Text) format.

## Syntax

```Haskell
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
