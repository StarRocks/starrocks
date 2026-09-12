---
displayed_sidebar: docs
description: "OGC WKB をネイティブ GEOMETRY 値に変換します。"
---

# ST_GeomFromWKB, ST_GeometryFromWKB

2 次元 OGC Well-Known Binary (WKB) 値をネイティブ `GEOMETRY` 値に変換します。

## 構文

```Haskell
GEOMETRY ST_GeomFromWKB(VARBINARY wkb)
GEOMETRY ST_GeometryFromWKB(VARBINARY wkb)
```

リトルエンディアンとビッグエンディアンの OGC WKB 入力、7 種類すべての OGC ジオメトリと各 `EMPTY` 形式をサポートします。EWKB、SRID、Z/M 座標はサポートされません。無効な入力は `NULL` を返します。

## キーワード

ST_GEOMFROMWKB,ST_GEOMETRYFROMWKB,ST,GEOMFROMWKB,GEOMETRYFROMWKB
