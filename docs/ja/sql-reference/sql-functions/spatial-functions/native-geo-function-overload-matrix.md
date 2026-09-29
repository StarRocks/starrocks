---
displayed_sidebar: docs
description: "ネイティブ GEOGRAPHY と GEOMETRY のオーバーロード、関数 ID、対応入力、実行時動作を示します。"
---

# ネイティブ GEO 関数のオーバーロードマトリックス

このページでは、初期 GEO 関数セットで導入されたネイティブ `GEOGRAPHY` と `GEOMETRY` 関数のオーバーロード契約を定義します。関数 ID は FE から BE へのディスパッチに使用されます。オーバーロードは SQL 論理型で解決され、値のディスクリプタは実行時に別途検証されます。WKB バイトから球面または平面の意味を推測することはありません。

## 共通動作

- ネイティブのコンストラクタとシリアライザは、2 次元 OGC の 7 種類すべて（`POINT`、`LINESTRING`、`POLYGON`、`MULTIPOINT`、`MULTILINESTRING`、`MULTIPOLYGON`、`GEOMETRYCOLLECTION`）に対応します。型付き `EMPTY` と空の子要素にも対応します。
- 引数が `NULL` の場合は `NULL` を返します。不正な WKT/WKB または未対応のコンストラクタ引数に対しても、コンストラクタは `NULL` を返します。
- ネイティブ計算関数には XY 値と有効なディスクリプタが必要です。未対応のファミリ、次元、ディスクリプタは制御されたエラーになります。ただし、下表で `EMPTY` が `NULL` を返すと明記されている場合を除きます。
- `GEOGRAPHY` と `GEOMETRY` は異なる論理型です。型を混在させる呼び出しに対応するオーバーロードはなく、拒否されます。
- `GEOGRAPHY` は `OGC:CRS84`、球面エッジ、経度/緯度の座標順序、SRID 4326 を使用します。`GEOMETRY` は平面セマンティクスとディスクリプタ内の明示的な CRS を使用します。このマトリックスの関数は再投影を行いません。

## コンストラクタ

| シグネチャ | 関数 ID | 戻り型 | 対応ファミリと次元 | CRS とディスクリプタの動作 |
| --- | ---: | --- | --- | --- |
| `ST_GeogFromText(VARCHAR wkt)` | 120020 | `GEOGRAPHY` | 2D の 7 ファミリすべてと `EMPTY` | ネイティブ `OGC:CRS84` 球面ディスクリプタと SRID 4326 を使用します。 |
| `ST_GeogFromText(VARCHAR wkt, INT srid)` | 120021 | `GEOGRAPHY` | 2D の 7 ファミリすべてと `EMPTY` | `srid` は 4326 である必要があります。 |
| `ST_GeomFromText(VARCHAR wkt, VARCHAR crs)` | 120022 | `GEOMETRY` | 2D の 7 ファミリすべてと `EMPTY` | `crs` は空でない文字列リテラルで、平面結果ディスクリプタになります。デフォルト CRS はありません。 |
| `ST_GeogFromWKB(VARBINARY wkb)` | 120030 | `GEOGRAPHY` | 2D の 7 ファミリすべてと `EMPTY`、両 WKB バイトオーダー | ネイティブ `OGC:CRS84` 球面ディスクリプタと SRID 4326 を使用します。EWKB は未対応です。 |
| `ST_GeogFromWKB(VARBINARY wkb, INT srid)` | 120031 | `GEOGRAPHY` | 2D の 7 ファミリすべてと `EMPTY`、両 WKB バイトオーダー | `srid` は 4326 である必要があります。EWKB は未対応です。 |
| `ST_GeomFromWKB(VARBINARY wkb, VARCHAR crs)` | 120032 | `GEOMETRY` | 2D の 7 ファミリすべてと `EMPTY`、両 WKB バイトオーダー | `crs` は空でない文字列リテラルで、平面結果ディスクリプタになります。EWKB と Z/M 座標は未対応です。 |

Geography 座標は対応する経度と緯度の範囲内である必要があります。Geometry コンストラクタは座標を再解釈せず、別の CRS に変換しません。[ST_GeogFromText](st_geogfromtext.md)、[ST_GeogFromWKB](st_geogfromwkb.md)、[ST_GeomFromText](st_geometryfromtext.md)、[ST_GeomFromWKB](st_geomfromwkb.md) を参照してください。

## シリアライザ

| シグネチャ | 関数 ID | 戻り型 | 動作 |
| --- | ---: | --- | --- |
| `ST_AsText(GEOGRAPHY value)` | 120040 | `VARCHAR` | 座標や入力ディスクリプタを変更せず WKT を出力します。 |
| `ST_AsText(GEOMETRY value)` | 120041 | `VARCHAR` | 座標や入力ディスクリプタを変更せず WKT を出力します。 |
| `ST_AsWKT(GEOGRAPHY value)` | 120050 | `VARCHAR` | ネイティブ `ST_AsText(GEOGRAPHY)` の別名です。 |
| `ST_AsWKT(GEOMETRY value)` | 120051 | `VARCHAR` | ネイティブ `ST_AsText(GEOMETRY)` の別名です。 |
| `ST_AsBinary(GEOGRAPHY value)` | 120060 | `VARBINARY` | WKB を出力します。論理型、CRS、エッジの意味は型メタデータのままで、EWKB には埋め込みません。 |
| `ST_AsBinary(GEOMETRY value)` | 120061 | `VARBINARY` | WKB を出力します。論理型と CRS は型メタデータのままで、EWKB には埋め込みません。 |
| `ST_AsWKB(GEOGRAPHY value)` | 120070 | `VARBINARY` | ネイティブ `ST_AsBinary(GEOGRAPHY)` の別名です。 |
| `ST_AsWKB(GEOMETRY value)` | 120071 | `VARBINARY` | ネイティブ `ST_AsBinary(GEOMETRY)` の別名です。 |

[ST_AsText と ST_AsWKT](st_astext.md)、[ST_AsBinary と ST_AsWKB](st_asbinary.md) を参照してください。

## 初期計算関数

| シグネチャ | 関数 ID | 戻り値と単位 | 対応する値 | `NULL`、`EMPTY`、ディスクリプタの動作 |
| --- | ---: | --- | --- | --- |
| `ST_X(GEOGRAPHY point)` | 120080 | `DOUBLE`、経度（度） | 空でない XY `POINT` | `NULL` は伝播します。`EMPTY`、非 `POINT`、未対応ディスクリプタはエラーです。 |
| `ST_X(GEOMETRY point)` | 120081 | `DOUBLE`、入力 CRS の単位 | 空でない XY `POINT` | `NULL` は伝播します。`EMPTY`、非 `POINT`、未対応ディスクリプタはエラーです。 |
| `ST_Y(GEOGRAPHY point)` | 120090 | `DOUBLE`、緯度（度） | 空でない XY `POINT` | `NULL` は伝播します。`EMPTY`、非 `POINT`、未対応ディスクリプタはエラーです。 |
| `ST_Y(GEOMETRY point)` | 120091 | `DOUBLE`、入力 CRS の単位 | 空でない XY `POINT` | `NULL` は伝播します。`EMPTY`、非 `POINT`、未対応ディスクリプタはエラーです。 |
| `ST_GeometryType(GEOGRAPHY value)` | 120170 | `VARCHAR` ファミリ名 | 対応するすべての XY ファミリ | `NULL` は伝播します。型付き `EMPTY` はファミリ名を保持します。未対応の次元またはディスクリプタはエラーです。 |
| `ST_GeometryType(GEOMETRY value)` | 120171 | `VARCHAR` ファミリ名 | 対応するすべての XY ファミリ | `NULL` は伝播します。型付き `EMPTY` はファミリ名を保持します。未対応の次元またはディスクリプタはエラーです。 |
| `ST_Distance(GEOGRAPHY lhs, GEOGRAPHY rhs)` | 120180 | `DOUBLE`、メートル | 球面 CRS84 契約の XY `POINT`/`POINT` | `NULL` または `EMPTY` は `NULL` です。未対応のファミリ、次元、ディスクリプタはエラーです。 |
| `ST_Distance(GEOMETRY lhs, GEOMETRY rhs)` | 120181 | `DOUBLE`、入力 CRS の単位 | ディスクリプタが一致する XY `POINT`/`POINT` | `NULL` または `EMPTY` は `NULL` です。未対応のファミリ、次元、互換性のないディスクリプタはエラーです。 |

[ST_X](st_x.md)、[ST_Y](st_y.md)、[ST_GeometryType](st_geometrytype.md)、[ST_Distance](st_distance.md) を参照してください。

## 包含関係の述語

| シグネチャ | 関数 ID | 境界の動作 |
| --- | ---: | --- |
| `ST_Contains(GEOGRAPHY polygon, GEOGRAPHY point)` | 120190 | 点がポリゴン内部にある場合のみ `true`。 |
| `ST_Contains(GEOMETRY polygon, GEOMETRY point)` | 120191 | 点がポリゴン内部にある場合のみ `true`。 |
| `ST_Within(GEOGRAPHY point, GEOGRAPHY polygon)` | 120200 | `ST_Contains` の逆で、境界を含みません。 |
| `ST_Within(GEOMETRY point, GEOMETRY polygon)` | 120201 | `ST_Contains` の逆で、境界を含みません。 |
| `ST_Covers(GEOGRAPHY polygon, GEOGRAPHY point)` | 120210 | 外周および内周の境界を含みます。 |
| `ST_Covers(GEOMETRY polygon, GEOMETRY point)` | 120211 | 外周および内周の境界を含みます。 |
| `ST_CoveredBy(GEOGRAPHY point, GEOGRAPHY polygon)` | 120220 | `ST_Covers` の逆で、境界を含みます。 |
| `ST_CoveredBy(GEOMETRY point, GEOMETRY polygon)` | 120221 | `ST_Covers` の逆で、境界を含みます。 |

これらのオーバーロードは、XY `POINT` と `POLYGON` または `MULTIPOLYGON` の組み合わせに対応します。`GEOGRAPHY` は球面 CRS84 エッジ、`GEOMETRY` は平面エッジで評価し、後者は一致するディスクリプタを必要とします。`NULL` は伝播し、`EMPTY` 入力は `false` を返します。未対応のファミリ、次元、ディスクリプタ、および `GEOGRAPHY`/`GEOMETRY` の混在呼び出しは拒否されます。[ST_Contains](st_contains.md)、[ST_Within](st_within.md)、[ST_Covers](st_covers.md)、[ST_CoveredBy](st_coveredby.md) を参照してください。

## レガシー互換性

ネイティブオーバーロードは、既存の `VARCHAR` 関数を再採番または置換しません。

| レガシーシグネチャ | 関数 ID |
| --- | ---: |
| `ST_X(VARCHAR)` | 120001 |
| `ST_Y(VARCHAR)` | 120002 |
| `ST_AsText(VARCHAR)` | 120004 |
| `ST_AsWKT(VARCHAR)` | 120005 |
| `ST_GeometryFromText(VARCHAR)` | 120006 |
| `ST_GeomFromText(VARCHAR)` | 120007 |
| `ST_Contains(VARCHAR, VARCHAR)` | 120014 |

## アップグレードと参照テストの契約

ローリングアップグレードでは、FE より先に BE をアップグレードしてください。これらの関数は通常の安定した関数 ID ディスパッチを使用し、独立した GEO バージョンゲートは導入しません。新しい FE と古い BE の組み合わせは、サポートされるアップグレード順序ではありません。

この契約は、オーバーロード解決、戻り型、レガシー互換性、関数 ID を確認する FE analyzer テストと、各ネイティブ ID を確認する BE registry テストで検証されます。既存の BE 関数テストは、定数、Nullable、可変入力、`EMPTY`、不正入力、ファミリ、次元、CRS、ディスクリプタの動作を網羅します。
