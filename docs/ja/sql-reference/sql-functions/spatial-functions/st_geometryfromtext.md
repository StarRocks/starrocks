---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "明示的な CRS を指定して WKT からネイティブ GEOMETRY を構築するか、従来の 1 引数形式を使用します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_GeometryFromText,ST_GeomFromText

WKT (Well Known Text) を対応するメモリジオメトリに変換します。

## 構文

```plaintext
GEOMETRY ST_GeometryFromText(VARCHAR wkt)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeometryFromText("LINESTRING (1 1, 2 2)"));
+---------------------------------------------------------+
| st_astext(st_geometryfromtext('LINESTRING (1 1, 2 2)')) |
+---------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                   |
+---------------------------------------------------------+
```

## キーワード

ST_GEOMETRYFROMTEXT,ST_GEOMFROMTEXT,ST,GEOMETRYFROMTEXT,GEOMFROMTEXT