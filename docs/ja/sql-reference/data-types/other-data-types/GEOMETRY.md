---
displayed_sidebar: docs
description: "2 次元 OGC ジオメトリ値をネイティブ WKB 形式で格納します。"
---

# GEOMETRY

`GEOMETRY` は 2 次元ジオメトリ値を正規化されたリトルエンディアン OGC Well-Known Binary (WKB) シリアライズ形式で格納します。

`POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON`、`GEOMETRYCOLLECTION` と各タイプの `EMPTY` 値をサポートします。[ST_GeomFromText](../../sql-functions/spatial-functions/st_geometryfromtext.md) または [ST_GeomFromWKB](../../sql-functions/spatial-functions/st_geometryfromwkb.md) で値を構築し、[ST_AsText](../../sql-functions/spatial-functions/st_astext.md) または [ST_AsBinary](../../sql-functions/spatial-functions/st_asbinary.md) でシリアライズします。

## 制限事項

- 2 次元 OGC WKT と WKB のみをサポートします。EWKT/EWKB、SRID、Z/M 座標はサポートされません。
- 正規化はバイナリシリアライズのみに適用され、座標、リング、コレクションの順序は正規化されません。
- 入力検証は構造のみを対象とし、トポロジーは検証しません。たとえば、自己交差などのトポロジーエラーは検出されません。
- `GEOMETRY` 列はキー、ソートキー、分散キー、パーティションキーに使用できません。
- `GEOMETRY` 値の比較、グループ化、順序付け、算術演算はサポートされません。
- 列のデフォルト値には `NULL` のみを使用できます。
