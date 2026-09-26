---
displayed_sidebar: docs
description: "MGRS文字列が示すグリッド方格の中心点の経度を返します。"
---

# MGRSToLng

[MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System)文字列をデコードし、参照されるグリッド方格の中心点の経度を返します。`geoToMGRS`の逆関数の一つです。`MGRSToLat`も参照してください。

入力は大文字・小文字を区別せず、空白は無視されます。

## Syntax

```Haskell
DOUBLE MGRSToLng(VARCHAR mgrs)
```

## パラメータ

- `mgrs`: MGRS参照文字列（例: `'31UDQ4825111935'`）。サポートされるデータ型: VARCHAR。

## 戻り値

グリッド方格中心の経度（度、WGS84）をDOUBLEで返します。引数がNULLまたは文字列が不正な場合はNULLを返します。

## 例

```sql
SELECT MGRSToLng('31UDQ4825111935');
+-----------------------------+
| MGRSToLng('31UDQ4825111935')|
+-----------------------------+
| 2.294495618908297           |
+-----------------------------+
```

## キーワード

MGRSTOLNG,MGRS,GEO,SPATIAL,UTM,LONGITUDE
