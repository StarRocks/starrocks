---
displayed_sidebar: docs
description: "有効なPoint型の場合、対応するY座標の値を返します。"
---

# ST_Y

ポイントが有効な Point 型の場合、対応する Y 座標の値を返します。

## 構文

```Haskell
DOUBLE ST_Y(POINT point)
```

## 例

```Plain Text
MySQL > SELECT ST_Y(ST_Point(24.7, 56.7));
+----------------------------+
| st_y(st_point(24.7, 56.7)) |
+----------------------------+
|                       56.7 |
+----------------------------+
```

## キーワード

ST_Y,ST,Y