---
displayed_sidebar: docs
---

# ST_Point

## Description

指定された X 座標と Y 座標に対応する Point を返します。現在、この値は球面セットでのみ意味があります。X/Y は経度/緯度に対応します。

> **注意**
>
> 直接 ST_Point() を選択すると、スタックする可能性があります。

## Syntax

```Haskell
POINT ST_Point(DOUBLE x, DOUBLE y)
```

## Examples

```Plain Text
MySQL > SELECT ST_AsText(ST_Point(24.7, 56.7));
+---------------------------------+
| st_astext(st_point(24.7, 56.7)) |
+---------------------------------+
| POINT (24.7 56.7)               |
+---------------------------------+
```

## keyword

ST_POINT,ST,POINT