---
displayed_sidebar: docs
description: "ネイティブ GEOGRAPHY 値を WKB としてシリアライズします。"
---

# ST_AsBinary, ST_AsWKB

ネイティブ `GEOGRAPHY` 値を Well-Known Binary（WKB）としてシリアライズします。`ST_AsWKB` は `ST_AsBinary` のエイリアスです。

## 構文

```Haskell
VARBINARY ST_AsBinary(GEOGRAPHY geography)
VARBINARY ST_AsWKB(GEOGRAPHY geography)
```

7 種類すべての OGC ジオメトリと `EMPTY` メンバーが保持されます。入力が `NULL` の場合は `NULL` を返します。

## 例

```SQL
SELECT hex(ST_AsBinary(ST_GeogFromText('POINT (1 2)')));
```

```text
0101000000000000000000F03F0000000000000040
```

## キーワード

ST_ASBINARY, ST_ASWKB, GEOGRAPHY, WKB
