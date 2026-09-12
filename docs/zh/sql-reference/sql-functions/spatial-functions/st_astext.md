---
displayed_sidebar: docs
description: "将原生 GEOMETRY 或旧版 geography 值转换为 WKT。"
---

# ST_AsText, ST_AsWKT

将原生 `GEOMETRY` 值或旧版 geography 值转换为 Well-Known Text (WKT)。

## 语法

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsText(VARCHAR legacy_geo)
VARCHAR ST_AsWKT(VARCHAR legacy_geo)
```

## 示例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('MULTIPOINT (1 2, 3 4)'));
+--------------------------------------------------------------+
| st_astext(st_geomfromtext('MULTIPOINT (1 2, 3 4)'))          |
+--------------------------------------------------------------+
| MULTIPOINT ((1 2), (3 4))                                    |
+--------------------------------------------------------------+
```

## 关键词

ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT
