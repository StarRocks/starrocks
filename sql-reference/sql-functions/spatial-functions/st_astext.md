# ST_AsText,ST_AsWKT

## description

### Syntax

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
```

Convert a geometric figure to WKT (Well Known Text) format.

## example

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
