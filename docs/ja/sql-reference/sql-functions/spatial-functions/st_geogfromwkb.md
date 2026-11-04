---
displayed_sidebar: docs
description: "WKB からネイティブ GEOGRAPHY 値を構築します。"
---

# ST_GeogFromWKB

Well-Known Binary（WKB）からネイティブ `GEOGRAPHY` 値を構築します。

## 構文

```Haskell
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb)
GEOGRAPHY ST_GeogFromWKB(VARBINARY wkb, INT srid)
```

## パラメータ

- `wkb`：リトルエンディアンまたはビッグエンディアンの 2 次元 OGC WKB 値。7 種類すべての OGC ジオメトリと `EMPTY` メンバーをサポートします。EWKB 拡張はサポートしません。
- `srid`：任意。空間参照識別子です。現在は `4326` のみをサポートし、省略時のデフォルト値も `4326` です。

座標は球面 `OGC:CRS84` セマンティクスを使用します。経度は `[-180, 180]`、緯度は `[-90, 90]` の範囲である必要があります。

## 戻り値

`GEOGRAPHY` 値を返します。いずれかの引数が `NULL`、WKB が無効、座標がサポート範囲外、または `srid` が `4326` 以外の場合は `NULL` を返します。

## 例

```SQL
SELECT ST_AsText(ST_GeogFromWKB(unhex('0101000000000000000000F03F0000000000000040')));
```

```text
POINT (1 2)
```

## キーワード

ST_GEOGFROMWKB, GEOGRAPHY, WKB
