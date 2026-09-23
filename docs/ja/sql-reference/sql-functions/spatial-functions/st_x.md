---
displayed_sidebar: docs
description: "点の X 座標を返します。GEOGRAPHY の場合、X は度単位の経度です。"
---

# ST_X

点の X 座標を返します。

`GEOGRAPHY` 値の場合、OGC:CRS84 の球面セマンティクスにおいて X は度単位の経度です。値は空でない `POINT` である必要があります。

## 構文

```SQL
DOUBLE ST_X(VARCHAR point)
DOUBLE ST_X(GEOGRAPHY point)
```

## 戻り値

入力が `NULL` の場合は `NULL` を返します。`GEOGRAPHY` が空、または `POINT` 以外の場合はエラーになります。サポートされていない次元またはディスクリプターもエラーになります。

## 例

```SQL
SELECT ST_X(ST_Point(24.7, 56.7));
-- 24.7

SELECT ST_X(ST_GeogFromText('POINT (24.7 56.7)'));
-- 24.7
```

## キーワード

ST_X,ST,X
