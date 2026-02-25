---
displayed_sidebar: docs
---

# ST_X

ポイントが有効な Point 型の場合、対応する X 座標の値を返します。

## 構文

```Haskell
DOUBLE ST_X(POINT point)
```

## 例

```Plain Text
MySQL > SELECT ST_X(ST_Point(24.7, 56.7));
+----------------------------+
| st_x(st_point(24.7, 56.7)) |
+----------------------------+
|                       24.7 |
+----------------------------+
```

## キーワード

ST_X,ST,X