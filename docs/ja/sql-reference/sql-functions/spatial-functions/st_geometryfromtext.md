---
displayed_sidebar: docs
description: "明示的な CRS を指定して WKT からネイティブ GEOMETRY を構築するか、従来の 1 引数形式を使用します。"
---

# ST_GeomFromText, ST_GeometryFromText

`ST_GeomFromText(wkt, crs)` は、2 次元 OGC Well-Known Text（WKT）からネイティブな平面 `GEOMETRY` 値を構築します。CRS は必須で、ネイティブ型記述子の一部になります。

既存の 1 引数 `ST_GeomFromText` と `ST_GeometryFromText` は変更されず、従来の `VARCHAR` 表現を返します。

## 構文

```Haskell
GEOMETRY ST_GeomFromText(VARCHAR wkt, VARCHAR crs)
VARCHAR ST_GeomFromText(VARCHAR wkt)
VARCHAR ST_GeometryFromText(VARCHAR wkt)
```

ネイティブ形式では、`crs` は空でない文字列リテラルである必要があります。デフォルト CRS はなく、暗黙的な座標変換や再投影も行いません。7 種類すべての 2D OGC ジオメトリ、`EMPTY`、および空の子要素をサポートします。無効な WKT または `NULL` の WKT は `NULL` を返します。

クエリでは、互換性のあるネイティブ空間値を `ARRAY` の要素、`MAP` の値、`STRUCT` のフィールドに使用できます。コンテナの構築、要素やフィールドへのアクセス、`cardinality` と `array_length`、`UNNEST` による配列の展開、`array_map` と `array_filter`、順序指定と `DISTINCT` を伴わない `array_agg` と `map_agg` がサポートされます。

`MAP` のキーに空間値を含めることはできません。比較、ソート、重複排除など、空間値の等価性、順序付け、ハッシュ化が必要な操作は拒否されます。ビュー、マテリアライズドビュー、SQL UDF、生成列、SQL プランベースライン、パイプなどの保存済み SQL 定義では、ネストされた空間式を使用できません。

コンテナをクライアントに返すには、空間値を含む要素やフィールドを事前に `ST_AsText` または `ST_AsBinary` で変換します。スカラーの `GEOGRAPHY` は直接返せます。スカラーの `GEOMETRY` にも明示的な変換が必要です。

```sql
SELECT array_map(x -> ST_AsText(x), [ST_GeomFromText('POINT (1 2)', 'EPSG:3857')]);
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_GeomFromText('LINESTRING (1 1, 2 2)', 'EPSG:3857'));
+----------------------------------------------------------------------------+
| st_astext(st_geomfromtext('LINESTRING (1 1, 2 2)', 'EPSG:3857'))           |
+----------------------------------------------------------------------------+
| LINESTRING (1 1, 2 2)                                                      |
+----------------------------------------------------------------------------+
```

## キーワード

ST_GEOMETRYFROMTEXT, ST_GEOMFROMTEXT, GEOMETRY, WKT, CRS
