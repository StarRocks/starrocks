---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "GEOGRAPHY または GEOMETRY の点の X 座標を返します。"
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# ST_X

ポイントが有効な Point 型の場合、対応する X 座標の値を返します。

## 構文

```plaintext
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