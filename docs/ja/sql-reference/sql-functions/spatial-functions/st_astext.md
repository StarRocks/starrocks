---
displayed_sidebar: docs
description: "GEOMETRY または GEOGRAPHY 値を WKT（Well-Known Text）形式に変換します。"
---

# ST_AsText, ST_AsWKT

`GEOMETRY` または `GEOGRAPHY` 値を WKT（Well-Known Text）形式に変換します。`ST_AsWKT` は `ST_AsText` のエイリアスです。

## 構文

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
VARCHAR ST_AsText(GEOGRAPHY geography)
VARCHAR ST_AsWKT(GEOMETRY geo)
VARCHAR ST_AsWKT(GEOGRAPHY geography)
```

`GEOGRAPHY` の場合、7 種類すべての OGC ジオメトリと `EMPTY` メンバーが保持されます。入力が `NULL` の場合は `NULL` を返します。

```SQL
SELECT ST_AsText(ST_GeogFromText('GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))'));
```

```text
GEOMETRYCOLLECTION (POINT EMPTY, LINESTRING (1 2, 3 4))
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## キーワード

ST_ASTEXT, ST_ASWKT, ST, ASTEXT, ASWKT
