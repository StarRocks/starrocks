---
displayed_sidebar: docs
---

# ST_LineFromText,ST_LineStringFromText

## Description

WKT (Well Known Text) を Line のメモリ表現に変換します。

## Syntax

```Haskell
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