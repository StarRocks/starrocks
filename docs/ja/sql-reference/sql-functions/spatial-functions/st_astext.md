---
displayed_sidebar: docs
---

# ST_AsText,ST_AsWKT

ジオメトリ図形を WKT (Well Known Text) 形式に変換します。

## 構文

```Haskell
VARCHAR ST_AsText(GEOMETRY geo)
```

## 例

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## キーワード

ST_ASTEXT,ST_ASWKT,ST,ASTEXT,ASWKT