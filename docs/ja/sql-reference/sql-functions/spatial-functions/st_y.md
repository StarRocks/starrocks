---
displayed_sidebar: docs
description: "点の Y 座標を返します。GEOGRAPHY の場合、Y は度単位の緯度です。"
---

# ST_Y

点の Y 座標を返します。

`GEOGRAPHY` 値の場合、OGC:CRS84 の球面セマンティクスにおいて Y は度単位の緯度です。値は空でない `POINT` である必要があります。

## 構文

```SQL
DOUBLE ST_Y(VARCHAR point)
DOUBLE ST_Y(GEOGRAPHY point)
```

## 戻り値

入力が `NULL` の場合は `NULL` を返します。`GEOGRAPHY` が空、または `POINT` 以外の場合はエラーになります。サポートされていない次元またはディスクリプターもエラーになります。

## 例

```SQL
SELECT ST_Y(ST_Point(24.7, 56.7));
-- 56.7

SELECT ST_Y(ST_GeogFromText('POINT (24.7 56.7)'));
-- 56.7
```

## キーワード

ST_Y,ST,Y
