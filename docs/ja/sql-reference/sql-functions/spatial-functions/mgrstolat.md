---
displayed_sidebar: docs
description: "MGRS文字列が示すグリッド方格の中心点の緯度を返します。"
---

# MGRSToLat

[MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)文字列をデコードし、参照されるグリッド方格の中心点の緯度を返します。`geoToMGRS`の逆関数の一つです。`MGRSToLng`も参照してください。

入力は大文字・小文字を区別せず、空白は無視されます。

## Syntax

```Haskell
DOUBLE MGRSToLat(VARCHAR mgrs)
```

## パラメータ

- `mgrs`: MGRS参照文字列（例: `'31UDQ4825111935'`）。サポートされるデータ型: VARCHAR。

## 戻り値

グリッド方格中心の緯度（度、WGS84）をDOUBLEで返します。引数がNULLまたは文字列が不正な場合はNULLを返します。

## 例

```sql
SELECT MGRSToLat('31UDQ4825111935');
+-----------------------------+
| MGRSToLat('31UDQ4825111935')|
+-----------------------------+
| 48.85822536113692           |
+-----------------------------+
```

## キーワード

MGRSTOLAT,MGRS,GEO,SPATIAL,UTM,LATITUDE
