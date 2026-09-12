---
displayed_sidebar: docs
description: "ネイティブ GEOMETRY または従来の geography 値を WKT に変換します。"
---

# ST_AsText,ST_AsWKT

ネイティブ `GEOMETRY` 値または従来の geography 値を Well-Known Text (WKT) に変換します。

## 構文

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsText(VARCHAR legacy_geo)
VARCHAR ST_AsWKT(VARCHAR legacy_geo)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('MULTIPOINT (1 2, 3 4)'));
+--------------------------------------------------------------+
| st_astext(st_geomfromtext('MULTIPOINT (1 2, 3 4)'))          |
+--------------------------------------------------------------+
| MULTIPOINT ((1 2), (3 4))                                    |
+--------------------------------------------------------------+
```

## キーワード

ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
