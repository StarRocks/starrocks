# ST_LineFromText,ST_LineStringFromText

## description

### Syntax

```Haskell
GEOMETRY ST_LineFromText(VARCHAR wkt)
```

Convert a WKT (Well Known Text) to a memory representation in the form of Line.

## example

```Plain Text
MySQL > SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## keyword

ST_LINEFROMTEXT,ST_LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT
