---
displayed_sidebar: docs
description: "WKT からネイティブ GEOGRAPHY 値を構築します。"
products: [oss, enterprise, cloud, anywhere]
---

# ST_GeogFromText

Well-Known Text（WKT）からネイティブ `GEOGRAPHY` 値を構築します。

## 構文

```Haskell
GEOGRAPHY ST_GeogFromText(VARCHAR wkt)
GEOGRAPHY ST_GeogFromText(VARCHAR wkt, INT srid)
```

## パラメータ

- `wkt`：2 次元 OGC WKT 値。7 種類すべての OGC ジオメトリと `EMPTY` メンバーをサポートします。
- `srid`：任意。空間参照識別子です。現在は `4326` のみをサポートし、省略時のデフォルト値も `4326` です。

座標は球面 `OGC:CRS84` セマンティクスを使用します。経度は `[-180, 180]`、緯度は `[-90, 90]` の範囲である必要があります。

## 戻り値

`GEOGRAPHY` 値を返します。いずれかの引数が `NULL`、WKT が無効、座標がサポート範囲外、または `srid` が `4326` 以外の場合は `NULL` を返します。

クエリでは、互換性のあるネイティブ空間値を `ARRAY` の要素、`MAP` の値、`STRUCT` のフィールドに使用できます。コンテナの構築、要素やフィールドへのアクセス、`cardinality` と `array_length`、`UNNEST` による配列の展開、`array_map` と `array_filter`、順序指定と `DISTINCT` を伴わない `array_agg` と `map_agg` がサポートされます。

`MAP` のキーに空間値を含めることはできません。比較、ソート、重複排除など、空間値の等価性、順序付け、ハッシュ化が必要な操作は拒否されます。ビュー、マテリアライズドビュー、SQL UDF、生成列、SQL プランベースライン、パイプなどの保存済み SQL 定義では、ネストされた空間式を使用できません。

コンテナをクライアントに返すには、空間値を含む要素やフィールドを事前に `ST_AsText` または `ST_AsBinary` で変換します。スカラーの `GEOGRAPHY` は直接返せます。スカラーの `GEOMETRY` にも明示的な変換が必要です。

```sql
SELECT array_map(x -> ST_AsText(x), [ST_GeogFromText('POINT (1 2)')]);
```

## 例

```SQL
SELECT ST_AsText(ST_GeogFromText('LINESTRING (1 2, 3 4)', 4326));
```

```text
LINESTRING (1 2, 3 4)
```

```SQL
SELECT ST_AsText(ST_GeogFromText('POINT EMPTY'));
```

```text
POINT EMPTY
```

## キーワード

ST_GEOGFROMTEXT, GEOGRAPHY, WKT
