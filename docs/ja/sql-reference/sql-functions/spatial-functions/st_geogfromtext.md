---
displayed_sidebar: docs
description: "WKT からネイティブ GEOGRAPHY 値を構築します。"
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
