---
displayed_sidebar: docs
---

# ST_LineFromText,ST_LineStringFromText

## 説明

WKT (Well Known Text) をメモリ表現の Line 形式に変換します。

## 構文

```Haskell
GEOMETRY ST_LineFromText(VARCHAR wkt)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_LineFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## キーワード

ST_LINEFROMTEXT,ST_LINESTRINGFROMTEXT,ST,LINEFROMTEXT,LINESTRINGFROMTEXT